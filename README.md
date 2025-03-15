# Пример работы с фалами с хранилища 🤗 Hugging Face Datasets массивом irlspbru/RFSD
![Python version](https://img.shields.io/badge/python-3.10%2B-blue)
[![Hugging Face](https://img.shields.io/badge/%F0%9F%A4%97_Hugging_Face-_oItsMineZ)](https://huggingface.co/datasets/irlspbru/RFSD)
<a href="https://huggingface.co/datasets/irlspbru/RFSD" target="_blank">
  <img src="https://img.shields.io/badge/%F0%9F%A4%97_Hugging_Face-_oItsMineZ" alt="Hugging Face">
</a>


В представленном описании массива данных [irlspbru/RFSD](https://github.com/irlcode/RFSD?tab=readme-ov-file#importing-the-data) не указывается оптимальная модель работы с данными. В случае применения предложенных алгоритмов возникают следующие нудобства:
- файлы скачиваются в директорию cash по умолчанию, потом самостоятельно назворачиваются в файлы формата *.arrow весом более 93 Гб
- сами данные не содержат указания на год, к которым относятся сведения
- при работе на устройствах с малой оперативной памятью в режимах по умолчанию возникает риск загрузки значительного объёма сведений в оперативную память, что приведёт в "крашу"

## Контролируемая загрузка
- `get_base_2.py` описывает "контролируемый процесс загрузки с последующим созданием файла с описанием хранимых файлов
- `use_base_2.py` показывает как работать с файлами вида '*.parquet' в режиме "ленивой" загрузки в вполне оперативно манипулировать данными без загрузки сведений и нагрузки на оперативку
