"""
Точка входа для мультиагентной системы «Конвейер генерации контента».

Запуск:
    python main.py
    python main.py "тема статьи"
"""

import sys
import os

# Добавляем корневую директорию проекта в sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm_content_pipeline.workflow import run_pipeline


def main() -> None:
    """Основная функция: запрашивает тему и запускает пайплайн."""
    print("=== Конвейер генерации контента (Мультиагентная система) ===")
    print("   Researcher -> Writer -> Editor -> Publisher\n")

    if len(sys.argv) > 1:
        topic = " ".join(sys.argv[1:])
        print(f"Тема: {topic}")
    else:
        topic = input("Введите тему статьи: ").strip()

    if not topic:
        print("Тема не может быть пустой.")
        return

    try:
        result_path = run_pipeline(topic)
        print(f"\nСтатья сохранена: {result_path}")
    except KeyboardInterrupt:
        print("\nВыполнение прервано пользователем.")
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
