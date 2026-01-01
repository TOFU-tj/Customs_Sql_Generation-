import os
import json
import logging
import torch
import asyncio

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message

from unsloth import FastLanguageModel

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

load_dotenv()
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("Нет TELEGRAM_BOT_TOKEN в .env")

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
MAX_SEQ_LENGTH = 2048

# -------------------------------------------------
# LOAD DB SCHEMA
# -------------------------------------------------

with open("BdDt.json", "r", encoding="utf-8") as f:
    DB_SCHEMA = json.dumps(json.load(f), ensure_ascii=False, indent=2)

# -------------------------------------------------
# LOAD MODEL + LORA
# -------------------------------------------------

logging.info("🔹 Loading base model...")

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name="unsloth/meta-llama-3.1-8b-unsloth-bnb-4bit",
    max_seq_length=MAX_SEQ_LENGTH,
    load_in_4bit=True,
)

logging.info("🔹 Loading LoRA adapter...")

model.load_adapter("customs_lora")

FastLanguageModel.for_inference(model)
model.to(DEVICE)

logging.info("✅ Model ready")

# -------------------------------------------------
# TELEGRAM INIT
# -------------------------------------------------

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()

# -------------------------------------------------
# SQL GENERATOR
# -------------------------------------------------

def generate_sql(user_query: str) -> str:
    prompt = f"""
Ты — эксперт по базе данных таможенных деклараций.
Твоя задача — сгенерировать ТОЛЬКО корректный SQL для SQLite.

СТРУКТУРА БАЗЫ ДАННЫХ:
{DB_SCHEMA}

ОБЯЗАТЕЛЬНЫЕ ПРАВИЛА:
1. Используй ТОЛЬКО существующие таблицы и поля
2. JOIN ТОЛЬКО по G071, G072, G073
3. Никаких комментариев
4. Никакого текста — ТОЛЬКО SQL
5. Используй CAST(... AS REAL) для чисел
6. dclplatr использовать ТОЛЬКО если запрошены платежи
7. Процедуры:
   - 40 → LIKE '40%'
   - 10 → LIKE '10%'

ЗАПРОС ПОЛЬЗОВАТЕЛЯ:
{user_query}

SQL:
"""

    inputs = tokenizer(prompt, return_tensors="pt").to(DEVICE)

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=512,
            temperature=0.01,
            do_sample=False,
        )

    text = tokenizer.decode(outputs[0], skip_special_tokens=True)

    # аккуратно вырезаем SQL
    if "SQL:" in text:
        text = text.split("SQL:", 1)[1]

    return text.strip()

# -------------------------------------------------
# HANDLERS
# -------------------------------------------------

@router.message(Command("start"))
async def start_handler(message: Message):
    await message.answer(
        "👋 Привет!\n\n"
        "Я SQL-ассистент для базы данных таможенных деклараций.\n"
        "Напиши запрос на русском — я верну готовый SQL.\n\n"
        "❗ Я отвечаю ТОЛЬКО SQL-кодом."
    )

@router.message()
async def handle_message(message: Message):
    if not message.text:
        await message.answer("Отправь текстовый запрос.")
        return

    await message.answer("🪄 Генерирую SQL...")

    try:
        sql = generate_sql(message.text)
        await message.answer(sql)
    except Exception as e:
        logging.exception("Ошибка генерации SQL")
        await message.answer(f"❌ Ошибка: {e}")

# -------------------------------------------------
# MAIN
# -------------------------------------------------

async def main():
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
