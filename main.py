import asyncio
import logging 
import sys
from os import getenv
import os
from dotenv import load_dotenv
from extractor import extract_highlights_from_pdf
from aiogram.types import FSInputFile
from aiogram.client.telegram import TelegramAPIServer
from bs4 import BeautifulSoup
import requests
from gofile_downloader import Download
from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.types import FSInputFile

load_dotenv()

TOKEN = str(getenv("BOT_TOKEN"))
print(TOKEN)


dp = Dispatcher() #Router

@dp.message(CommandStart())
async def command_start_handler(message:Message) -> None:
    await message.answer(f"  مرحبًا {html.bold(message.from_user.full_name)}")


@dp.message()
async def highlights_extractor(message: Message) -> None:
    """
    document: file_id='BQACAgQAAxkBAAO9Z7v2m6F2jYi4Ii_wwOmuOP8MrPwAAskZAALA-uBR2GLStb4jUMI2BA' 
    file_unique_id='AgADyRkAAsD64FE' 
    thumbnail=PhotoSize(file_id='AAMCBAADGQEAA71nu_aboXaNiLgiL_DA6a44_wys_AACyRkAAsD64FHYYtK1viNQwgEAB20AAzYE',
    file_unique_id='AQADyRkAAsD64FFy',
    width=320,
    height=180,
    file_size=1071) 
    file_name='640قيم_كبير.png' 
    mime_type='image/png' 
    file_size=809 
    thumb={'file_id': 'AAMCBAADGQEAA71nu_aboXaNiLgiL_DA6a44_wys_AACyRkAAsD64FHYYtK1viNQwgEAB20AAzYE', 
    'file_unique_id': 'AQADyRkAAsD64FFy', 
    'file_size': 1071,
    'width': 320, 
    'height': 180}
    """
    try:
        if message.document:
            if message.document.mime_type == message.document.mime_type:#"application/pdf": # is pdf 
                # download pdf 
                doc = message.document
                file_name  = doc.file_name
                file_id = message.document.file_id
                path = f"books_files/{file_name}"
                await message.bot.download(file=doc,destination=path)
        elif "https://gofile.io/d" in message.text: # user sent a gofile url
            
            path = Download(message.text,download_path=f"books_files/")
            
        # TODO save highlights in a user searchable specific way 
        highlights = extract_highlights_from_pdf(pdf_path=path)
        print(highlights)

        # send highlights-
        
        for image in highlights:
            img = FSInputFile(str(image[0]))
            await message.answer_photo(photo=img,caption=f"كتاب:{file_name}\n صفحه:{image[1]}")

    except Exception as e:
        raise e
        # But not all the types is supported to be copied so need to handle it
        await message.answer("صيغه غير مدعومه")



async def main() -> None:
    # Initialize Bot instance with default bot properties which will be passed to all API calls
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    # And the run events dispatching
    await dp.start_polling(bot,skip_updates=True)

if __name__ == "__main__":
    try:
        os.mkdir("books_files")
    except:
        pass
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())