# Практическая работа 3 — ML Pipeline на Spark MLlib

> **Вариант 20** |


---

## Описание задачи

**Бизнес-кейс.** Компания **HealthTech** (аналог Endomondo/Strava).  
Задача — построить прогностическую модель для определения **пола пользователя** по поведенческим паттернам тренировок (пульс, скорость, высота).

### Задания варианта 20

| № | Задание | Реализация |
|---|---|---|
| 1 | **Feature Engineering:** сложные агрегаты (концептуальное скользящее среднее пульса), целевая переменная `gender` | UDF для `hr_trend` и `hr_rolling_std` из массивов |
| 2 | **Моделирование:** `CrossValidator` (Grid Search) для подбора гиперпараметров | 3-fold CV, сетка 3×3×2 = 18 комбинаций |
| 3 | **Бизнес-метрики:** Оценка влияния тюнинга на ROC-AUC vs затраты времени | Сравнение Baseline ↔ CrossValidator |

---

## Структура репозитория

```
lab03_ml/
├── README.md              ← Этот файл
├── lab_03_ml.ipynb        ← Основной ноутбук (Setup → ETL → Features → ML → Eval)
├── Report.md              ← Отчёт с метриками и бизнес-выводами
├── data/                  ← Автоматически создаётся при запуске
│   └── endomondoHR.json   ← Скачивается с Google Drive
└── figures/               ← Автоматически создаётся при запуске
    ├── fig1_metrics_comparison.png
    ├── fig2_roc_confusion.png
    ├── fig3_feature_gridsearch.png
    └── fig4_distributions.png
```

---

## Источник данных

**Датасет.** `endomondoHR.json`  
**Описание.** Данные о тренировках пользователей (пульс, скорость, высота, вид спорта, пол).  
**Источник.** [Google Drive](https://drive.google.com/file/d/1yiAp1fFDy3wSqUR0X_btCZPtuczbLwCe/view?usp=drive_link)

> ⚠️ Ноутбук автоматически скачивает файл при запуске — вручную загружать не нужно.

---

## Инструкция по запуску

### Вариант A. Google Colab (рекомендуется)

1. Загрузите `lab_03_ml.ipynb` в Google Colab
2. Запустите все ячейки подряд (`Runtime → Run all`)
3. Зависимости устанавливаются автоматически в первой ячейке

### Вариант B. Docker — образ `ds_mgpu_Hadoop3+spark_3_4`

```bash
# Запуск контейнера
docker run -it --rm -p 8888:8888 \
  -v $(pwd):/home/jovyan/work \
  ds_mgpu_Hadoop3+spark_3_4

# Откройте браузер: http://localhost:8888
# Откройте файл: work/lab_03_ml.ipynb
```

### Вариант C. Локальная установка

```bash
# Установка зависимостей
pip install pyspark==3.4.1 gdown findspark matplotlib seaborn scikit-learn

# Запуск Jupyter
jupyter notebook lab_03_ml.ipynb
```

### Требования к Python-окружению

| Библиотека | Версия | Назначение |
|---|---|---|
| `pyspark` | 3.4.x | Spark + MLlib |
| `gdown` | latest | Загрузка с Google Drive |
| `findspark` | latest | Инициализация Spark |
| `matplotlib` | ≥ 3.5 | Визуализация |
| `seaborn` | ≥ 0.12 | Heatmaps |
| `scikit-learn` | ≥ 1.0 | ROC curve, confusion matrix |
| `numpy`, `pandas` | latest | Обработка данных |

---

## Ключевые технические решения

### Feature Engineering (Задание 1)

Поля `heart_rate` и `speed` — это массивы временных измерений. Для реализации **концептуального скользящего среднего** применяются два UDF:

- **`hr_trend`** — разница между средним значением последнего и первого квартиля массива пульса. Отражает *направление* изменения пульса за тренировку.
- **`hr_rolling_std`** — среднее стандартное отклонение в скользящем окне шириной 5 точек. Отражает *вариабельность* пульса.

### CrossValidator Grid Search (Задание 2)

Сетка гиперпараметров `RandomForestClassifier`:



---

## Авторские права

Датасет предоставлен в образовательных целях. Оригинальный источник: Endomondo workout dataset (Kaggle / публичные данные).
