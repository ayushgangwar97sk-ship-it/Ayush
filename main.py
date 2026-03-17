import asyncio
import logging
import os
import sys
import traceback
import random
import json
import re
from datetime import datetime

# Third-party imports
from telethon import TelegramClient, events, functions, types, Button
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError,
    PasswordHashInvalidError, PhoneNumberInvalidError,
    UserAlreadyParticipantError, InviteHashExpiredError,
    FloodWaitError, MediaCaptionTooLongError, ChannelPrivateError
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, GetMessagesViewsRequest, SendReactionRequest
from telethon.tl.functions.account import UpdateStatusRequest
from telethon.tl.functions.phone import LeaveGroupCallRequest 
import motor.motor_asyncio

# --- IMPORT CONFIGURATION ---
try:
    from config import API_ID, API_HASH, BOT_TOKEN, MONGO_URL, LOG_CHANNEL, ADMIN_IDS as ENV_ADMINS
    # Try to import START_IMG, use default if not found
    try:
        from config import START_IMG
    except ImportError:
        START_IMG = "https://i.ibb.co/0yZT5SFv/Auto-Rt-Lv-Tools.png" # placeholder image
except ImportError:
    print("❌ Critical Error: config.py not found! Please create it.")
    sys.exit(1)

# --- CONSTANTS ---
EMOJIS = ["👍", "🔥", "❤️", "🥰", "👏", "😁", "🎉", "🤩", "🐳", "🌭", "⚡️"]
DELAY_RANGE = (2, 8)

# --- JITTER CONFIGURATION ---
# Default values (Overwritten by DB)
JITTER_CONFIG = {
    "ENABLED": True,
    "START_DELAY": (60, 180),  # Seconds to wait AFTER join before starting chaos
    "PERCENTAGE": 0.20,        # 20% of bots will jitter
    "LEAVE_DELAY": (6, 8),     # Updated: 6-8 seconds stay
    "REJOIN_DELAY": (15, 60)   # How long to wait outside before re-joining
}

# --- LOGGING SETUP ---
logging.basicConfig(
    format='[%(levelname)s] %(asctime)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("ManagerBot")

# --- DATABASE HANDLER (MongoDB) ---
class Database:
    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)
        self.db = self.client['telegram_bot_db']
        self.sessions = self.db['sessions']
        self.settings = self.db['settings']

    async def add_session(self, user_id, name, session_string):
        await self.sessions.update_one(
            {"user_id": user_id},
            {"$set": {"name": name, "session_string": session_string, "active": True}, 
             "$setOnInsert": {"added_at": datetime.now(), "role": None}},
            upsert=True
        )

    async def update_role(self, user_id, role):
        await self.sessions.update_one(
            {"user_id": user_id},
            {"$set": {"role": role}}
        )

    async def get_all_sessions(self):
        cursor = self.sessions.find({"active": True})
        return await cursor.to_list(length=None)

    async def remove_session(self, user_id):
        await self.sessions.delete_one({"user_id": user_id})

    # --- Config Methods ---
    async def get_config(self):
        doc = await self.settings.find_one({"_id": "config"})
        return doc if doc else {}

    async def update_jitter_status(self, enabled: bool):
        await self.settings.update_one(
            {"_id": "config"},
            {"$set": {"jitter_enabled": enabled}},
            upsert=True
        )

    # --- Target Chat Methods ---
    async def get_target_chats(self):
        doc = await self.settings.find_one({"_id": "config"})
        return doc.get("target_chats", []) if doc else []

    async def add_target_chat(self, chat_id):
        await self.settings.update_one(
            {"_id": "config"},
            {"$addToSet": {"target_chats": chat_id}},
            upsert=True
        )

    async def remove_target_chat(self, chat_id):
        await self.settings.update_one(
            {"_id": "config"},
            {"$pull": {"target_chats": chat_id}}
        )

    async def clear_target_chats(self):
        await self.settings.update_one(
            {"_id": "config"},
            {"$set": {"target_chats": []}}
        )

    # --- Admin Methods ---
    async def get_admins(self):
        doc = await self.settings.find_one({"_id": "config"})
        db_admins = doc.get("admins", []) if doc else []
        return list(set(ENV_ADMINS + db_admins))

    async def add_admin(self, user_id):
        await self.settings.update_one(
            {"_id": "config"},
            {"$addToSet": {"admins": user_id}},
            upsert=True
        )
    
    async def remove_admin(self, user_id):
        await self.settings.update_one(
            {"_id": "config"},
            {"$pull": {"admins": user_id}}
        )

db = Database()

# --- GLOBAL STATE ---
active_userbots = {}
login_states = {}
TARGET_CHATS = []  # Populated from DB on startup
ADMIN_LIST = []    # Populated from DB on startup
RUNTIME_CHATS = set() # Temporary targets for Custom Live feature

# Stores asyncio tasks for jitter logic: {chat_id: [task1, task2]}
jitter_tasks_registry = {} 

# --- ROLE ASSIGNMENT GLOBALS ---
GLOBAL_JITTER_IDS = set()
GLOBAL_STABLE_IDS = set()

# --- MASTER BOT INSTANCE ---
bot = TelegramClient('master_bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)


# --- HELPER FUNCTIONS ---
async def log_to_channel(message):
    if LOG_CHANNEL:
        try:
            await bot.send_message(LOG_CHANNEL, f"**[LOG]** {message}")
        except Exception as e:
            logger.error(f"Failed to log to channel: {e}")

async def refresh_global_config():
    """Reloads settings from DB to Global Variables."""
    global TARGET_CHATS, ADMIN_LIST, JITTER_CONFIG
    
    config = await db.get_config()
    TARGET_CHATS = config.get("target_chats", [])
    ADMIN_LIST = list(set(ENV_ADMINS + config.get("admins", [])))
    
    # Load Jitter Setting
    if "jitter_enabled" in config:
        JITTER_CONFIG["ENABLED"] = config["jitter_enabled"]
    
    logger.info(f"Config Refreshed. Targets: {len(TARGET_CHATS)}, Admins: {len(ADMIN_LIST)}, Jitter: {JITTER_CONFIG['ENABLED']}")

def is_admin(user_id):
    return user_id in ADMIN_LIST

async def assign_roles():
    """
    Assigns Permanent Roles using Database Persistence.
    Only assigns new roles to accounts that don't have one.
    """
    global GLOBAL_JITTER_IDS, GLOBAL_STABLE_IDS
    GLOBAL_JITTER_IDS.clear()
    GLOBAL_STABLE_IDS.clear()
    
    sessions = await db.get_all_sessions()
    if not sessions: return

    # 1. Sort existing roles
    unassigned = []
    current_jitter_count = 0
    
    for s in sessions:
        uid = s['user_id']
        role = s.get('role')
        
        if role == 'jitter':
            GLOBAL_JITTER_IDS.add(uid)
            current_jitter_count += 1
        elif role == 'stable':
            GLOBAL_STABLE_IDS.add(uid)
        else:
            unassigned.append(uid)

    # 2. Assign roles to new/unassigned bots
    if unassigned:
        total_bots = len(sessions)
        target_jitter = int(total_bots * JITTER_CONFIG["PERCENTAGE"])
        
        # Calculate how many more jitter bots we need
        needed = target_jitter - current_jitter_count
        
        # Ensure at least 1 jitter bot if we have > 3 bots and none exist
        if needed <= 0 and current_jitter_count == 0 and total_bots > 3:
            needed = 1
            
        random.shuffle(unassigned)
        
        # Split unassigned into new jitter and new stable
        new_jitter = unassigned[:max(0, needed)]
        new_stable = unassigned[max(0, needed):]
        
        # Update Globals and Database
        for uid in new_jitter:
            GLOBAL_JITTER_IDS.add(uid)
            await db.update_role(uid, 'jitter')
            
        for uid in new_stable:
            GLOBAL_STABLE_IDS.add(uid)
            await db.update_role(uid, 'stable')
            
    logger.info(f"🎭 Permanent Roles Loaded: {len(GLOBAL_STABLE_IDS)} Stable, {len(GLOBAL_JITTER_IDS)} Jitter")

# --- USERBOT ACTION: JOIN VIA LINK ---
async def join_channel_via_link(client, link):
    """Joins a channel using a public or private link."""
    try:
        if '+' in link or 'joinchat' in link:
            try:
                hash_val = link.split('+')[-1] if '+' in link else link.split('joinchat/')[-1]
                hash_val = hash_val.replace('/', '') 
                await client(ImportChatInviteRequest(hash_val))
                return True, "Joined (Private)"
            except UserAlreadyParticipantError:
                return True, "Already Joined"
            except InviteHashExpiredError:
                return False, "Link Expired"
        else:
            username = link.split('/')[-1]
            try:
                await client(JoinChannelRequest(username))
                return True, "Joined (Public)"
            except UserAlreadyParticipantError:
                return True, "Already Joined"
    except FloodWaitError as e:
        return False, f"FloodWait ({e.seconds}s)"
    except Exception as e:
        return False, str(e)[:50]

# --- USERBOT LOGIC ---
async def get_call_object(client, chat_id):
    """Helper to get the current call object for a chat."""
    try:
        entity = await client.get_entity(chat_id)
        if isinstance(entity, (types.Channel, types.InputPeerChannel)):
            full_chat = await client(functions.channels.GetFullChannelRequest(entity))
        else:
            full_chat = await client(functions.messages.GetFullChatRequest(entity.id))
        
        if not hasattr(full_chat, 'full_chat') or not hasattr(full_chat.full_chat, 'call'):
            return None
        return full_chat.full_chat.call
    except:
        return None

async def join_channel_live(client, chat_id):
    """Attempts to join a live voice chat/stream and forces Online status."""
    try:
        # 1. Get Call Object
        call = await get_call_object(client, chat_id)
        if not call:
            return False

        # 2. Join Logic
        join_as = await client.get_input_entity(client.me.id)
        
        # FIX: SSRC must be deterministic (constant) for the same bot
        # This prevents the bot from "refreshing" the connection params which causes a drop/rejoin flicker
        ssrc = (client.me.id) % 2147483647 

        await client(functions.phone.JoinGroupCallRequest(
            call=call,
            join_as=join_as,
            params=types.DataJSON(data=json.dumps({
                "ufrag": "", "pwd": "", "fingerprints": [], "ssrc": ssrc,
                "ssrc-groups": [], "payload-types": [], "rtcp-fb": [],
                "fec-channels": [], "rtcp-mux": True
            })),
            muted=True
        ))

        # 3. FORCE ONLINE STATUS
        await client(UpdateStatusRequest(offline=False))
        return True

    except Exception as e:
        error_msg = str(e)
        # 400 ERROR is GOOD. It means we are already in the call securely.
        if "GROUPCALL_ALREADY_JOINED" in error_msg:
            try: await client(UpdateStatusRequest(offline=False))
            except: pass
            return True
        return False

# --- POST VIEW & REACTION LOGIC ---
async def process_view_post(client, link):
    """
    Views a post and adds a random reaction.
    Handles Public (t.me/user/123) and Private (t.me/c/12345/123) links.
    """
    try:
        # Regex to parse link
        regex = r"(?:https?://)?(?:www\.)?t\.me/(?:c/)?([^/]+)/(\d+)"
        match = re.search(regex, link)
        if not match:
            return False, "Invalid Link Format"
            
        chat_identifier = match.group(1)
        msg_id = int(match.group(2))
        
        # Determine Entity
        try:
            if chat_identifier.isdigit():
                # Private Chat ID (ensure -100 prefix)
                entity_id = int(f"-100{chat_identifier}")
                entity = await client.get_entity(entity_id)
            else:
                # Public Username
                entity = await client.get_entity(chat_identifier)
        except ValueError:
             # Try simpler approach for public
             entity = await client.get_entity(chat_identifier)
        except Exception:
             return False, "Chat not found/Not joined"

        # 1. VIEW (Get Message)
        # This fetches message data to ensure it exists
        msgs = await client.get_messages(entity, ids=[msg_id])
        if not msgs:
            return False, "Message not found"

        # FIX: Explicitly request to increment the view counter
        try:
            await client(GetMessagesViewsRequest(
                peer=entity,
                id=[msg_id],
                increment=True
            ))
        except Exception as e:
            # Log but don't fail, sometimes it fails if already viewed
            pass

        # 2. REACTION
        try:
            emoji = random.choice(EMOJIS)
            await client(SendReactionRequest(
                peer=entity,
                msg_id=msg_id,
                reaction=[types.ReactionEmoji(emoticon=emoji)]
            ))
            await client(UpdateStatusRequest(offline=False))
            return True, f"Viewed + Reacted {emoji}"
        except Exception as e:
            return True, f"Viewed (React Failed: {str(e)[:20]})"

    except Exception as e:
        return False, str(e)[:40]

# --- JITTER MODE LOGIC ---

async def jitter_loop(client, chat_id):
    """
    Infinite loop for the Jitter Squad.
    Joins -> Stays a bit -> Leaves -> Waits -> Repeats.
    """
    bot_name = client.me.first_name
    logger.info(f"🎢 Jitter Loop STARTED for {bot_name} in {chat_id}")
    
    try:
        while True:
            # Double check role inside loop for safety
            if client.me.id not in GLOBAL_JITTER_IDS:
                logger.warning(f"🛡️ {bot_name} is STABLE but inside jitter loop! Exiting loop.")
                break

            if not JITTER_CONFIG["ENABLED"]:
                logger.info("Jitter Disabled Globally. Exiting loop.")
                break

            try:
                # 1. Stay joined for random time
                stay_duration = random.randint(*JITTER_CONFIG["LEAVE_DELAY"])
                await asyncio.sleep(stay_duration)

                # 2. LEAVE
                call = await get_call_object(client, chat_id)
                if call:
                    await client(LeaveGroupCallRequest(call=call, source=0))
                    logger.info(f"👋 {bot_name} Left Stream (Jitter)")
                else:
                    logger.warning(f"Live ended in {chat_id}, stopping jitter for {bot_name}")
                    break

                # 3. Wait outside for random time
                wait_outside = random.randint(*JITTER_CONFIG["REJOIN_DELAY"])
                await asyncio.sleep(wait_outside)

                # 4. RE-JOIN
                joined = await join_channel_live(client, chat_id)
                if joined:
                    logger.info(f"🔙 {bot_name} Re-Joined Stream (Jitter)")
                else:
                    break

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in jitter loop for {bot_name}: {e}")
                await asyncio.sleep(10)
    except asyncio.CancelledError:
        pass

async def start_jitter_mode(chat_id):
    """
    Orchestrates the Jitter logic using PRE-ASSIGNED roles.
    """
    if not JITTER_CONFIG["ENABLED"]:
        return

    # Check if we are already jittering this chat
    if chat_id in jitter_tasks_registry:
        return

    # LOCK THE CHAT IMMEDIATELY
    jitter_tasks_registry[chat_id] = []

    # 1. Wait for delay
    start_delay = random.randint(*JITTER_CONFIG["START_DELAY"])
    logger.info(f"⏳ Jitter Scheduler: Waiting {start_delay}s before starting chaos in {chat_id}")
    
    try:
        await asyncio.sleep(start_delay)
    except asyncio.CancelledError:
        if chat_id in jitter_tasks_registry:
            del jitter_tasks_registry[chat_id]
        return

    # 2. Validate state
    if chat_id not in jitter_tasks_registry:
        return
        
    # Check config again in case it changed during sleep
    if not JITTER_CONFIG["ENABLED"]:
        del jitter_tasks_registry[chat_id]
        return

    # 3. Select the 20% based on PERMANENT ROLES
    jitter_squad = []
    
    # Iterate through the globally assigned Jitter IDs
    for uid in GLOBAL_JITTER_IDS:
        if uid in active_userbots:
            jitter_squad.append(active_userbots[uid])

    # Log stable count
    stable_count = 0
    for uid in GLOBAL_STABLE_IDS:
        if uid in active_userbots:
            stable_count += 1

    logger.info(f"🎲 Jitter Mode Active for {chat_id}: {stable_count} Stable, {len(jitter_squad)} Chaos")
    await log_to_channel(f"🎲 **Jitter Mode Activated** in `{chat_id}`\n🛡️ **Stable:** {stable_count} bots\n🎢 **Chaos:** {len(jitter_squad)} bots")

    # 4. Start tasks
    tasks = []
    for client in jitter_squad:
        task = asyncio.create_task(jitter_loop(client, chat_id))
        tasks.append(task)

    jitter_tasks_registry[chat_id] = tasks

async def stop_jitter_mode(chat_id):
    """Cancels jitter tasks when stream ends."""
    if chat_id in jitter_tasks_registry:
        tasks = jitter_tasks_registry[chat_id]
        for t in tasks:
            t.cancel()
        del jitter_tasks_registry[chat_id]
        logger.info(f"🛑 Stopped Jitter Mode for {chat_id}")

async def scan_lives_on_startup(client):
    """Checks all target chats for existing lives when bot starts."""
    if not TARGET_CHATS:
        return
    for chat_id in TARGET_CHATS:
        try:
            # All bots join initially
            joined = await join_channel_live(client, chat_id)
            if joined:
                 asyncio.create_task(start_jitter_mode(chat_id))
            await asyncio.sleep(2)
        except Exception:
            pass


async def start_userbot(session_string, user_id, name):
    """Starts a single userbot from a string session."""
    try:
        client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            logger.warning(f"Session for {name} is invalid/expired.")
            await log_to_channel(f"⚠️ Session for {name} (ID: {user_id}) is expired. Removing.")
            await db.remove_session(user_id)
            return None

        me = await client.get_me()
        client.me = me
        active_userbots[me.id] = client
        # --- EVENT: AUTO REACTION & VIEW ---
        @client.on(events.NewMessage(incoming=True))
        async def reaction_handler(event):
            if not event.is_channel: return
            if TARGET_CHATS and event.chat_id not in TARGET_CHATS: return
            if event.message.action: return

            await asyncio.sleep(random.randint(*DELAY_RANGE))
            try:
                # 1. Increment View Counter Explicitly
                await client(GetMessagesViewsRequest(
                    peer=event.chat_id,
                    id=[event.id],
                    increment=True
                ))

                # 2. Send Reaction
                emoji = random.choice(EMOJIS)
                await client(functions.messages.SendReactionRequest(
                    peer=event.peer_id,
                    msg_id=event.id,
                    reaction=[types.ReactionEmoji(emoticon=emoji)]
                ))
                
                await client(UpdateStatusRequest(offline=False))
            except Exception:
                pass

        # --- EVENT: AUTO JOIN LIVE ---
        @client.on(events.NewMessage(incoming=True))
        async def live_detection_handler(event):
            if not event.is_channel: return
            if TARGET_CHATS and event.chat_id not in TARGET_CHATS: return
            if not event.message.action: return
            if not isinstance(event.message.action, types.MessageActionGroupCall): return
            
            action = event.message.action
            
            # LIVE STARTED
            if hasattr(action, 'duration') and action.duration is None:
                chat = await client.get_entity(event.chat_id)
                chat_title = getattr(chat, 'title', f'Chat {event.chat_id}')
                await log_to_channel(f"🎬 **Live Stream Started** in **{chat_title}**")
                
                # Immediate join for everyone
                await asyncio.sleep(random.randint(5, 15))
                await join_channel_live(client, event.chat_id)

                # Trigger scheduler
                asyncio.create_task(start_jitter_mode(event.chat_id))

            # LIVE ENDED
            elif hasattr(action, 'duration') and action.duration:
                 await stop_jitter_mode(event.chat_id)

        # --- PERIODIC CHECK & STATUS KEEP-ALIVE ---
        async def periodic_live_check():
            while True:
                # 1. Keep Online Status
                try:
                    await client(UpdateStatusRequest(offline=False))
                except: pass

                # 2. Check Lives (Redundancy)
                # Combine Permanent Targets and Runtime (Custom Live) Targets
                all_chats = set(TARGET_CHATS) | RUNTIME_CHATS
                
                if all_chats:
                    for chat_id in all_chats:
                        try:
                            # ROLE BASED CHECK LOGIC
                            my_id = client.me.id
                            
                            # Case A: I am a JITTER BOT
                            if my_id in GLOBAL_JITTER_IDS:
                                # Do NOT manually join/check. My jitter_loop task handles me.
                                # Just ensure the manager is running.
                                if chat_id not in jitter_tasks_registry:
                                    asyncio.create_task(start_jitter_mode(chat_id))
                                
                            # Case B: I am a STABLE BOT (or role not assigned yet)
                            else:
                                # Aggressively ensure I am joined
                                await join_channel_live(client, chat_id)
                                # Also ensure manager is running (triggers jitter for others)
                                if chat_id not in jitter_tasks_registry:
                                    asyncio.create_task(start_jitter_mode(chat_id))

                        except: pass
                        # Small delay between checking chats
                        await asyncio.sleep(2)
                
                # REJOIN CHECK INTERVAL REDUCED TO 30s
                # faster re-joins if disconnected
                await asyncio.sleep(30) 

        asyncio.create_task(periodic_live_check())
        return client

    except Exception as e:
        logger.error(f"Failed to start userbot {name}: {e}")
        return None


async def reload_userbots():
    """Disconnect all userbots and reload from database."""
    # Stop all jitter tasks
    for chat_id in list(jitter_tasks_registry.keys()):
        await stop_jitter_mode(chat_id)

    for uid, client in list(active_userbots.items()):
        try:
            if client.is_connected():
                await client.disconnect()
        except: pass
    active_userbots.clear()
    
    await refresh_global_config()

    sessions = await db.get_all_sessions()
    tasks = []
    for s in sessions:
        tasks.append(start_userbot(s['session_string'], s['user_id'], s.get('name', 'Unknown')))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    successful_bots = [r for r in results if r is not None]
    
    # ASSIGN PERMANENT ROLES NOW (Requires `await`)
    await assign_roles()
    
    return len(successful_bots)


# --- MASTER BOT INTERFACE & UI ---

@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    if not is_admin(event.sender_id):
        return await event.respond("⛔ **Access Denied.** You are not an authorized administrator.")
    
    # Send the welcome image + dashboard
    # We use 'respond' here to create a fresh message with the image.
    await show_dashboard(event, new_message=True)


async def show_dashboard(event, edit=False, new_message=False):
    sessions = await db.get_all_sessions()
    active_count = len(active_userbots)
    total_bots = len(sessions)
    offline_count = total_bots - active_count

    target_msg = "🌍 **Global** (All Chats)" if not TARGET_CHATS else f"🎯 **{len(TARGET_CHATS)}** Targets Set"
    jitter_status = "✅ Active" if JITTER_CONFIG['ENABLED'] else "❌ Disabled"
    jitter_icon = "⏸️" if JITTER_CONFIG['ENABLED'] else "▶️"

    text = (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 **M A N A G E R   P R O   V 2**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**📊 SYSTEM STATUS**\n"
        f"🟢 **Active:** `{active_count}`   🔴 **Offline:** `{offline_count}`\n"
        f"👥 **Admins:** `{len(ADMIN_LIST)}`   {target_msg}\n\n"
        f"**⚙️ CONFIGURATION**\n"
        f"🎢 **Jitter Mode:** `{jitter_status}`\n"
        f"🛡️ **Strategy:** `80% Stable / 20% Chaos (6-8s)`\n\n"
        f"👇 **Select an action below:**"
    )

    buttons = [
        [Button.inline("➕ Add Account", b"add_menu"), Button.inline("🗑️ Remove", b"remove_menu")],
        [Button.inline("🔗 Mass Join", b"join_link_menu"), Button.inline("📺 Custom Live", b"custom_live_menu")],
        [Button.inline(f"{jitter_icon} Toggle Jitter", b"toggle_jitter"), Button.inline("👁️ Post Views", b"view_menu")],
        [Button.inline("🎯 Target List", b"target_menu"), Button.inline("👥 Admins", b"admin_menu")],
        [Button.inline("🔄 Reload System", b"reload"), Button.inline("📊 Stats", b"stats_menu")]
    ]

    if new_message:
        await event.respond(text, file=START_IMG, buttons=buttons)
    elif edit:
        try:
            await event.edit(text, buttons=buttons)
        except Exception:
            await event.edit(text, buttons=buttons, file=None) 
    else:
        await event.respond(text, buttons=buttons)


@bot.on(events.CallbackQuery)
async def callback_handler(event):
    if not is_admin(event.sender_id):
        return await event.answer("⛔ Access Denied", alert=True)

    data = event.data.decode('utf-8')
    chat_id = event.chat_id

    if data == "main_menu":
        if chat_id in login_states: del login_states[chat_id]
        await show_dashboard(event, edit=True)

    # --- TOGGLE JITTER ---
    elif data == "toggle_jitter":
        new_state = not JITTER_CONFIG["ENABLED"]
        await db.update_jitter_status(new_state)
        await refresh_global_config()
        
        # Stop tasks if disabled
        if not new_state:
            for cid in list(jitter_tasks_registry.keys()):
                await stop_jitter_mode(cid)
        
        await event.answer(f"Jitter Mode {'Enabled' if new_state else 'Disabled'}!", alert=True)
        await show_dashboard(event, edit=True)

    # --- CUSTOM LIVE MENU ---
    elif data == "custom_live_menu":
        login_states[chat_id] = {"step": "CUSTOM_LIVE_LINK"}
        text = (
            "📺 **CUSTOM LIVE STREAM JOIN**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "This will force bots to join a specific live stream.\n"
            "**Note:** These bots will be **STABLE** (No Jitter).\n\n"
            "1️⃣ Send the **Invite Link** (Public or Private).\n\n"
            "🚫 _Type /cancel to abort._"
        )
        await event.edit(text, buttons=[[Button.inline("❌ Cancel", b"main_menu")]])

    # --- VIEW POST MENU ---
    elif data == "view_menu":
        login_states[chat_id] = {"step": "VIEW_POST_LINK"}
        text = (
            "👁️ **POST VIEWS & REACTIONS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Increase views and add random reactions to a post.\n"
            "**Supports:** Public & Private Channels.\n\n"
            "1️⃣ Send the **Post Link**.\n"
            "Ex: `https://t.me/channel/123`\n\n"
            "🚫 _Type /cancel to abort._"
        )
        await event.edit(text, buttons=[[Button.inline("❌ Cancel", b"main_menu")]])

    # --- STATISTICS ---
    elif data == "stats_menu":
        try:
            msg = await event.edit("🔄 **Fetching Real-time Statistics...**")
        except:
            msg = event.message

        stats_text = f"📊 **NETWORK STATISTICS**\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        if not active_userbots:
             stats_text += "❌ **No active bots found.**"
        else:
            for uid, client in active_userbots.items():
                try:
                    me = client.me
                    status_icon = "🟢" if client.is_connected() else "🔴"
                    role_tag = "🛡️ Stable" if uid in GLOBAL_STABLE_IDS else "🎢 Jitter" if uid in GLOBAL_JITTER_IDS else "❔ Unknown"
                    
                    entry = (
                        f"👤 **{me.first_name}**\n"
                        f"├ 🆔 `{uid}`\n"
                        f"├ 📶 Status: {status_icon} Online\n"
                        f"└ 🎭 Role: {role_tag}\n\n"
                    )
                    stats_text += entry
                except Exception:
                    stats_text += f"🔸 **Unknown User** (Error)\n"

        stats_text += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        stats_text += f"**Active Chaos Loops:**\n"
        
        if not jitter_tasks_registry:
            stats_text += "💤 No active jitter tasks running."
        else:
            for cid in jitter_tasks_registry:
                count = len(jitter_tasks_registry[cid])
                stats_text += f"• Chat `{cid}`: 🔥 **{count}** bots looping\n"
        
        if len(stats_text) > 4000:
            stats_text = stats_text[:4000] + "\n\n⚠️ **TRUNCATED: List too long.**"

        if len(stats_text) > 950:
            try:
                await msg.delete()
            except: pass
            await event.respond(stats_text, buttons=[[Button.inline("🔙 Main Menu", b"main_menu")]])
        else:
            try:
                await msg.edit(stats_text, buttons=[[Button.inline("🔙 Main Menu", b"main_menu")]])
            except MediaCaptionTooLongError:
                await msg.delete()
                await event.respond(stats_text, buttons=[[Button.inline("🔙 Main Menu", b"main_menu")]])

    # --- JOIN VIA LINK MENU ---
    elif data == "join_link_menu":
        login_states[chat_id] = {"step": "JOIN_LINK"}
        text = (
            "🔗 **MASS JOIN OPERATION**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "**Instructions:**\n"
            "Send a Public or Private channel link. All active bots will attempt to join immediately.\n\n"
            "✅ **Supported Formats:**\n"
            "• `https://t.me/channelname`\n"
            "• `@channelname`\n"
            "• `https://t.me/+AbCdEfGhIjK` (Private)\n\n"
            "🚫 _Type /cancel to abort this operation._"
        )
        await event.edit(text, buttons=[[Button.inline("❌ Cancel", b"main_menu")]])

    # --- ADMIN MANAGEMENT ---
    elif data == "admin_menu":
        admins = await db.get_admins()
        text = (
            "👥 **ADMINISTRATOR MANAGEMENT**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Current authorized users:\n\n"
        )
        for i, adm in enumerate(admins, 1):
            text += f"{i}. 👤 `{adm}`\n"
        
        buttons = [
            [Button.inline("➕ Add Admin", b"add_admin_step"), Button.inline("➖ Remove Admin", b"rm_admin_menu")],
            [Button.inline("🔙 Main Menu", b"main_menu")]
        ]
        await event.edit(text, buttons=buttons)

    elif data == "add_admin_step":
        login_states[chat_id] = {"step": "ADD_ADMIN"}
        await event.edit("➕ **ADD NEW ADMIN**\n\nPlease send the **Telegram User ID** of the person you want to authorize.\n\n_Type /cancel to abort._", 
                         buttons=[[Button.inline("❌ Cancel", b"admin_menu")]])

    elif data == "rm_admin_menu":
        admins = await db.get_admins()
        buttons = []
        for adm in admins:
            if adm != event.sender_id:
                buttons.append([Button.inline(f"❌ {adm}", f"rm_adm_{adm}")])
        
        buttons.append([Button.inline("🔙 Back", b"admin_menu")])
        await event.edit("➖ **REMOVE ADMIN**\nSelect a user to revoke access:", buttons=buttons)

    elif data.startswith("rm_adm_"):
        adm_id = int(data.split("_")[2])
        await db.remove_admin(adm_id)
        await refresh_global_config()
        await event.answer(f"✅ Removed Admin {adm_id}", alert=True)
        await callback_handler(event) 

    elif data == "add_menu":
        text = (
            "📲 **ADD ACCOUNT**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Please select a login method:\n\n"
            "1️⃣ **Phone Number:** Standard login via OTP code.\n"
            "2️⃣ **Session String:** Fast login using Pyrogram/Telethon string.\n"
        )
        await event.edit(text, buttons=[
            [Button.inline("📱 Phone Number", b"add_phone")],
            [Button.inline("📝 Session String", b"add_string")],
            [Button.inline("🔙 Cancel", b"main_menu")]
        ])

    elif data == "reload":
        msg = await event.edit("🔄 **System Reload Initiated...**\n\n• Disconnecting bots...\n• Refreshing database...\n• Reassigning roles...")
        count = await reload_userbots()
        await msg.edit(f"✅ **System Reloaded Successfully!**\n\n🤖 Active Bots: `{count}`", buttons=[[Button.inline("🔙 Main Menu", b"main_menu")]])

    elif data == "target_menu":
        status = "🌍 **Global (All Chats)**" if not TARGET_CHATS else f"🎯 **{len(TARGET_CHATS)} Specific Target(s)**"
        text = (
            f"🎯 **TARGET CONFIGURATION**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Current Mode: {status}\n\n"
            f"Bots will only auto-react and auto-join lives in the chats listed here. If empty, they work everywhere.\n"
        )
        buttons = [
            [Button.inline("➕ Add Target ID", b"add_tgt_id"), Button.inline("➖ Remove Target", b"rm_tgt_menu")],
            [Button.inline("🗑️ Clear All Targets", b"clear_tgt")],
            [Button.inline("🔙 Main Menu", b"main_menu")]
        ]
        await event.edit(text, buttons=buttons)

    elif data == "add_tgt_id":
        login_states[chat_id] = {"step": "ADD_TARGET"}
        await event.edit("➕ **ADD TARGET CHAT**\n\nSend the **Channel/Group ID** (starting with -100...).\n\n_Type /cancel to abort._", buttons=[[Button.inline("❌ Cancel", b"main_menu")]])

    elif data == "clear_tgt":
        await db.clear_target_chats()
        await refresh_global_config()
        await event.answer("✅ All targets cleared! Switching to Global Mode.", alert=True)
        await callback_handler(event)

    elif data == "rm_tgt_menu":
        if not TARGET_CHATS: return await event.answer("⚠️ No targets configured.", alert=True)
        buttons = [[Button.inline(f"❌ {tid}", f"rm_tgt_{tid}")] for tid in TARGET_CHATS]
        buttons.append([Button.inline("🔙 Back", b"target_menu")])
        await event.edit("➖ **REMOVE TARGET**\nSelect an ID to remove:", buttons=buttons)

    elif data.startswith("rm_tgt_"):
        await db.remove_target_chat(int(data.split("_")[2]))
        await refresh_global_config()
        await event.answer("✅ Target removed.", alert=True)
        if TARGET_CHATS: await callback_handler(event)
        else: await event.edit("✅ **All targets removed!**", buttons=[[Button.inline("🔙 Back", b"target_menu")]])

    elif data == "add_phone":
        login_states[chat_id] = {"step": "PHONE"}
        await event.edit("📱 **LOGIN VIA PHONE**\n\nEnter the account phone number in international format.\nExample: `+1234567890`", buttons=[[Button.inline("❌ Cancel", b"main_menu")]])

    elif data == "add_string":
        login_states[chat_id] = {"step": "STRING"}
        await event.edit("📝 **LOGIN VIA STRING**\n\nPaste the Pyrogram/Telethon session string below.", buttons=[[Button.inline("❌ Cancel", b"main_menu")]])

    elif data == "list":
        sessions = await db.get_all_sessions()
        text = "📜 **REGISTERED ACCOUNTS**\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for s in sessions:
            status = "🟢" if s['user_id'] in active_userbots else "🔴"
            text += f"{status} **{s.get('name', 'Unknown')}**\n   └ ID: `{s['user_id']}`\n"
        await event.edit(text, buttons=[[Button.inline("🔙 Main Menu", b"main_menu")]])

    elif data == "remove_menu":
        sessions = await db.get_all_sessions()
        buttons = [[Button.inline(f"❌ {s.get('name','User')} ({s['user_id']})", f"rm_{s['user_id']}")] for s in sessions]
        buttons.append([Button.inline("🔙 Back", b"main_menu")])
        await event.edit("🗑️ **DELETE ACCOUNT**\nSelect an account to remove from the database:", buttons=buttons)

    elif data.startswith("rm_"):
        uid = int(data.split("_")[1])
        await db.remove_session(uid)
        if uid in active_userbots:
            await active_userbots[uid].disconnect()
            del active_userbots[uid]
        await event.answer("✅ Account deleted successfully!", alert=True)
        await show_dashboard(event, edit=True)


@bot.on(events.NewMessage)
async def wizard_handler(event):
    if not is_admin(event.sender_id) or not event.text:
        return

    chat_id = event.chat_id
    text = event.text.strip()

    if text == "/cancel":
        if chat_id in login_states:
            if 'client' in login_states[chat_id]:
                try: await login_states[chat_id]['client'].disconnect()
                except: pass
            del login_states[chat_id]
        await event.respond("🚫 **Operation Cancelled.**", buttons=[[Button.inline("🔙 Main Menu", b"main_menu")]])
        return

    if chat_id not in login_states:
        return

    state = login_states[chat_id]
    step = state['step']

    try:
        # --- WIZARD: CUSTOM LIVE JOIN ---
        if step == "CUSTOM_LIVE_LINK":
            state['link'] = text
            state['step'] = "CUSTOM_LIVE_COUNT"
            await event.respond("🔢 **How many bots?**\n\nEnter a number (e.g., `10`, `20`) or type `all`.", buttons=[[Button.inline("❌ Cancel", b"main_menu")]])

        elif step == "CUSTOM_LIVE_COUNT":
            try:
                if text.lower() == "all":
                    count = len(active_userbots)
                else:
                    count = int(text)
            except ValueError:
                return await event.respond("❌ Invalid number. Try again.")

            link = state['link']
            msg = await event.respond(f"⏳ **Joining Live Stream...**\nTarget: `{count}` bots\nLink: `{link}`\n\n_Processing..._")
            
            # Select Bots (Convert dict values to list)
            all_bots = list(active_userbots.values())
            random.shuffle(all_bots)
            selected_bots = all_bots[:count]

            success = 0
            
            for client in selected_bots:
                # First ensure joined to channel
                await join_channel_via_link(client, link)
                await asyncio.sleep(1)
                
                # Extract Chat ID for joining live
                try:
                    # Simple heuristic to get ID from link for join_channel_live
                    if 'joinchat' in link or '+' in link:
                         # Private links are hard to resolve to ID immediately without checking dialogs
                         # Skip detailed check, try to resolve via entity
                         chat = await client.get_input_entity(link)
                         chat_id_resolved = chat.channel_id if hasattr(chat, 'channel_id') else chat.chat_id
                    else:
                        username = link.split('/')[-1]
                        chat = await client.get_entity(username)
                        chat_id_resolved = chat.id
                    
                    # Add to RUNTIME CHATS so they stay connected
                    RUNTIME_CHATS.add(chat_id_resolved)
                    
                    # Join Live
                    res = await join_channel_live(client, chat_id_resolved)
                    if res: success += 1
                except Exception as e:
                    logger.error(f"Custom Join Fail: {e}")
                
                await asyncio.sleep(random.randint(1, 3))

            del login_states[chat_id]
            await msg.delete()
            await event.respond(f"✅ **Operation Complete**\n\n👥 Bots Joined Live: `{success}/{len(selected_bots)}`\nℹ️ _These bots are in Stable Mode (No Jitter)._", buttons=[[Button.inline("🔙 Main Menu", b"main_menu")]])

        # --- WIZARD: VIEW POST ---
        elif step == "VIEW_POST_LINK":
            state['link'] = text
            state['step'] = "VIEW_POST_COUNT"
            await event.respond("🔢 **How many bots?**\n\nEnter a number (e.g., `50`, `100`) or type `all`.", buttons=[[Button.inline("❌ Cancel", b"main_menu")]])

        elif step == "VIEW_POST_COUNT":
            try:
                if text.lower() == "all":
                    count = len(active_userbots)
                else:
                    count = int(text)
            except ValueError:
                return await event.respond("❌ Invalid number. Try again.")
            
            link = state['link']
            msg = await event.respond(f"⏳ **Processing Views & Reactions...**\nTarget: `{count}` bots\n\n_Please wait..._")

            all_bots = list(active_userbots.values())
            random.shuffle(all_bots)
            selected_bots = all_bots[:count]

            success = 0
            
            for client in selected_bots:
                res, status_msg = await process_view_post(client, link)
                if res: success += 1
                await asyncio.sleep(random.uniform(0.5, 2)) # Fast but safe delay

            del login_states[chat_id]
            await msg.delete()
            await event.respond(f"✅ **Views Delivered**\n\n👁️ Success: `{success}/{len(selected_bots)}`", buttons=[[Button.inline("🔙 Main Menu", b"main_menu")]])


        # --- EXISTING STEPS ---
        elif step == "ADD_ADMIN":
            try:
                new_admin_id = int(text)
                await db.add_admin(new_admin_id)
                await refresh_global_config()
                del login_states[chat_id]
                await event.respond(f"✅ **Success!**\nUser `{new_admin_id}` is now an admin.", buttons=[[Button.inline("👥 Admin Menu", b"admin_menu")]])
            except ValueError:
                await event.respond("❌ **Error:** Invalid ID. Please send a numeric User ID.")

        elif step == "JOIN_LINK":
            msg = await event.respond(f"⏳ **Processing Join Requests...**\nLink: `{text}`\n\n_Please wait while all bots attempt to join._")
            
            results = []
            success_count = 0
            
            for uid, client in active_userbots.items():
                status, join_msg = await join_channel_via_link(client, text)
                icon = "✅" if status else "❌"
                results.append(f"{icon} **{client.me.first_name}**: {join_msg}")
                if status: success_count += 1
                await asyncio.sleep(random.randint(2, 5))
            
            # Generate Report
            report = (
                f"📝 **JOIN REPORT**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"**Target:** `{text}`\n"
                f"**Success:** {success_count}/{len(active_userbots)}\n\n"
            ) + "\n".join(results)
            
            if len(report) > 4000:
                report = report[:4000] + "\n...(truncated)"
                
            del login_states[chat_id]
            await msg.delete()
            await event.respond(report, buttons=[[Button.inline("🔙 Main Menu", b"main_menu")]])

        elif step == "ADD_TARGET":
            try:
                target_id = int(text)
                await db.add_target_chat(target_id)
                await refresh_global_config()
                del login_states[chat_id]
                await event.respond(f"✅ **Target Configured:** `{target_id}`", buttons=[[Button.inline("🎯 Target Menu", b"target_menu")]])
            except ValueError:
                await event.respond("❌ **Error:** Invalid ID format.")

        elif step == "STRING":
            try:
                temp_client = TelegramClient(StringSession(text), API_ID, API_HASH)
                await temp_client.connect()
                if await temp_client.is_user_authorized():
                    user = await temp_client.get_me()
                    await db.add_session(user.id, user.first_name, text)
                    await temp_client.disconnect()
                    del login_states[chat_id]
                    await event.respond(f"✅ **Account Added!**\nWelcome, **{user.first_name}**.", buttons=[[Button.inline("🏠 Main Menu", b"main_menu")]])
                    await reload_userbots()
                else:
                    await event.respond("❌ **Error:** Invalid Session String. Please try again.")
                    await temp_client.disconnect()
            except Exception as e:
                await event.respond(f"❌ **Error:** {e}")

        elif step == "PHONE":
            temp_client = TelegramClient(StringSession(), API_ID, API_HASH)
            await temp_client.connect()
            try:
                await temp_client.send_code_request(text)
                state['client'] = temp_client
                state['phone'] = text
                state['step'] = "CODE"
                await event.respond("📩 **OTP Sent!**\n\nPlease enter the code you received:", buttons=[[Button.inline("❌ Cancel", b"main_menu")]])
            except Exception as e:
                await event.respond(f"❌ **Error:** {e}")
                await temp_client.disconnect()

        elif step == "CODE":
            temp_client = state['client']
            phone = state['phone']
            try:
                code = text.replace(' ', '')
                await temp_client.sign_in(phone, code)
                user = await temp_client.get_me()
                session_str = StringSession.save(temp_client.session)
                await db.add_session(user.id, user.first_name, session_str)
                await temp_client.disconnect()
                del login_states[chat_id]
                await event.respond(f"✅ **Login Successful!**\nAccount **{user.first_name}** has been added.", buttons=[[Button.inline("🏠 Dashboard", b"main_menu")]])
                await reload_userbots()
            except SessionPasswordNeededError:
                state['step'] = "PASSWORD"
                await event.respond("🔐 **Two-Step Verification Detected**\n\nPlease enter your 2FA Password:", buttons=[[Button.inline("❌ Cancel", b"main_menu")]])
            except Exception as e:
                await event.respond(f"❌ **Error:** {e}")
                await temp_client.disconnect()

        elif step == "PASSWORD":
            temp_client = state['client']
            try:
                await temp_client.sign_in(password=text)
                user = await temp_client.get_me()
                session_str = StringSession.save(temp_client.session)
                await db.add_session(user.id, user.first_name, session_str)
                await temp_client.disconnect()
                del login_states[chat_id]
                await event.respond(f"✅ **Login Successful!**\nAccount **{user.first_name}** has been added.", buttons=[[Button.inline("🏠 Dashboard", b"main_menu")]])
                await reload_userbots()
            except Exception as e:
                await event.respond(f"❌ **Error:** {e}")
                await temp_client.disconnect()

    except Exception as e:
        logger.error(traceback.format_exc())
        await event.respond("❌ **System Error.** Check logs.")

async def main():
    print("--- Manager Bot V2 Starting ---")
    await bot.start()
    
    # Initial Config Load
    await refresh_global_config()
    print(f"✅ Admins: {ADMIN_LIST}")
    
    count = await reload_userbots()
    print(f"✅ Loaded {count} Userbots")
    
    await log_to_channel(f"🚀 **System Online**\nBots: `{count}`\nAdmins: `{len(ADMIN_LIST)}`")
    await bot.run_until_disconnected()

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n⚠️ Bot stopped")
