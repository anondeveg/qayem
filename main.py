import os
import asyncio
from os import getenv
from dotenv import load_dotenv
from telethon import TelegramClient, events
from extractor import extract_highlights_from_pdf
from telethon.tl.types import DocumentAttributeFilename
from telethon.tl.types import MessageMediaDocument
from telethon.sessions import StringSession

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

API_ID = int(getenv('API_ID'))
API_HASH = getenv('API_HASH')
BOT_TOKEN = getenv('BOT_TOKEN')

# Ensure download directory exists
os.makedirs('books_files', exist_ok=True)

# Create and start the bot client
client = TelegramClient('bot_session', API_ID,
                        API_HASH).start(bot_token=BOT_TOKEN)


@client.on(events.NewMessage(pattern='/start'))
async def handler_start(event):
    user = await event.get_sender()
    name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    await event.reply(f"مرحبًا {name}")


@client.on(events.NewMessage(func=lambda e: bool(e.media)))
async def handler_document(event):
    """
    Handles incoming documents (PDFs), downloads, extracts highlights, and replies with images.
    """
    try:
        media = event.message.media
        # Check if it's a document with .pdf extension
        if isinstance(media, MessageMediaDocument):
            # Find the f lename attribute
            pdf_attr = next((a for a in media.document.attributes
                             if isinstance(a, DocumentAttributeFilename)), None)
            if pdf_attr and pdf_attr.file_name.lower().endswith('.pdf'):
                file_name = pdf_attr.file_name
                file_path = f"books_files/{file_name}"
                # Download the PDF
                await client.download_media(media, file_path)
                await event.reply(f"يتم تحميل الملف {file_name}")
                # Extract highlights
                highlights = extract_highlights_from_pdf(pdf_path=file_path)

                # Send each highlighted image back
                for img_path, page in highlights:
                    await client.send_file(
                        event.chat_id,
                        img_path,
                        caption=f"كتاب: {file_name}\nصفحة: {page}"
                    )
            else:
                await client.reply("لا أستطيع معالجة ملف بغير صيغة الPDF")
    except Exception as e:
        # You might want to log the error here
        await event.reply("صيغة غير مدعومة أو حدث خطأ أثناء المعالجة.")

if __name__ == '__main__':
    print("Bot is running...")
    client.run_until_disconnected()
