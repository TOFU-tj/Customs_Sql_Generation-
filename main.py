import os
import json
import replicate
import logging
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from aiogram import Router
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)

load_dotenv()

# Токены
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
REPLICATE_API_TOKEN = os.getenv("REPLICATE_API_TOKEN")

if not BOT_TOKEN or not REPLICATE_API_TOKEN:
    raise ValueError("Укажите TELEGRAM_BOT_TOKEN и REPLICATE_API_TOKEN в .env")

os.environ["REPLICATE_API_TOKEN"] = REPLICATE_API_TOKEN

# Загружаем схему
with open("BdDt.json", "r", encoding="utf-8") as f:
    DB_SCHEMA = json.dumps(json.load(f), ensure_ascii=False)

# Инициализация
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()

def generate_sql(user_query: str) -> str:
    prompt = (
        "Ты — эксперт по базе данных таможенных деклараций. Сгенерируй ТОЛЬКО корректный SQL для SQLite.\n\n"
        
        "СТРУКТУРА БАЗЫ ДАННЫХ:\n"
        + DB_SCHEMA + "\n\n"
        
        "ПРАВИЛА:\n"
        "1. Связывай таблицы ТОЛЬКО по трём полям: G071, G072, G073 (они есть во всех таблицах).\n"
        "2. Направление перемещения: dclhead.G011 ('ИМ' или 'ЭК').\n"
        "3. Страна происхождения: dcltovar.G34 (цифровой код: Италия='380', Германия='276', Китай='156').\n"
        "4. Вес нетто (кг): dcltovar.G38 → всегда CAST(G38 AS REAL).\n"
        "5. Статистическая стоимость (USD): dcltovar.G46 → всегда CAST(G46 AS REAL).\n"
        "6. Сумма платежа: dclplatr.G474 → ИСПОЛЬЗУЙ ТОЛЬКО если в запросе есть слова 'платёж', 'пошлина', 'начисленный'.\n"
        "7. ИТС = статистическая стоимость / вес нетто = CAST(G46 AS REAL) / NULLIF(CAST(G38 AS REAL), 0).\n"
        "8. Процедура: dcltovar.G37 → '40' = LIKE '40%', '10' = LIKE '10%'.\n"
        "9. Группировка по ТН ВЭД: 2 знака → SUBSTR(G33,1,2), 4 → SUBSTR(G33,1,4), 6 → SUBSTR(G33,1,6).\n"
        "10. Никогда не используй dclplatr, если не запрашивается 'платёж'.\n"
        "11. Только SELECT, только SQL, без пояснений, без комментариев.\n\n"
        
        "ПРИМЕРЫ:\n"
        "Пример 1 (платежи):\n"
        "Запрос: сумма начисленного платежа для Италии на таможне 10122*\n"
        "SQL: SELECT SUM(CAST(p.G474 AS REAL)) FROM dclhead h JOIN dcltovar t ON h.G071=t.G071 AND h.G072=t.G072 AND h.G073=t.G073 JOIN dclplatr p ON t.G071=p.G071 AND t.G072=p.G072 AND t.G073=p.G073 WHERE h.G071 LIKE '10122%' AND t.G34='380';\n\n"
        
        "Пример 2 (ИТС, БЕЗ платежей):\n"
        "Запрос: ИТС для товаров на таможне 10125020, направление ЭК, процедура 10\n"
        "SQL: SELECT SUBSTR(t.G33,1,4), t.G31_1, SUM(CAST(t.G38 AS REAL)), SUM(CAST(t.G46 AS REAL)), ROUND(SUM(CAST(t.G46 AS REAL))/NULLIF(SUM(CAST(t.G38 AS REAL)),0),2) FROM dclhead h JOIN dcltovar t ON h.G071=t.G071 AND h.G072=t.G072 AND h.G073=t.G073 WHERE h.G071='10125020' AND h.G011='ЭК' AND t.G37 LIKE '10%' GROUP BY 1,2;\n\n"
        
        "Теперь обработай запрос:\n" + user_query + "\n\nSQL:"
    )

    output = replicate.run(
        "meta/meta-llama-3-8b-instruct",
        input={
            "prompt": prompt,
            "max_tokens": 1024,
            "temperature": 0.01,  # ещё точнее
            "top_p": 0.9,
            "prompt_template": "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\n{prompt}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"
        }
    )
    return "".join(output).strip()

    
# Обработчик команды /start
@router.message(Command("start"))
async def start_handler(message: Message):
    await message.answer(
        "👋 Привет! Я — SQL-ассистент для базы данных таможенных деклараций.\n\n"
        "Отправь мне любой запрос на русском языке, и я сгенерирую для тебя готовый SQL-код.\n\n"
    )

# Обработчик ЛЮБОГО текстового сообщения, КРОМЕ команд
@router.message()
async def handle_message(message: Message):
    if not message.text:
        await message.answer("Пожалуйста, отправь текстовый запрос.")
        return

    logging.info(f"Получен запрос: {message.text}")
    await message.answer("🪄 Генерирую SQL... (10–20 сек)")

    try:
        sql = generate_sql(message.text)
        # Очищаем от возможных лишних символов
        if sql.startswith("```sql"):
            sql = sql[7:]
        if sql.endswith("```"):
            sql = sql[:-3]
        await message.answer(sql.strip())
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")

# Запуск
async def main():
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())