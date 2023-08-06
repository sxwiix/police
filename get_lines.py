import pandas as pd
from tabulate import tabulate

# Открываем DataFrame
data_frame = pd.read_csv(r'C:\Users\hqmnd\Desktop\police-department-calls-for-service.csv')

# Создаем переменную и получаем ввод
search_id = input("Введите id строки: ")

# Преобразуем в int
search_id = int(search_id)

# Создаем маску для поиска
mask = data_frame["Crime Id"] == search_id

# Получаем индекс найденной строки
index_id = data_frame[mask].index[0]

# Диапазон -5 строк вверх
start_index_before = max(0, index_id - 5)
end_index_before = index_id
result_before = data_frame.iloc[start_index_before:end_index_before]

# Выводим найденный id
result_found = data_frame[mask]

# Получаем диапазон +5 вниз
start_index_after = index_id + 1
end_index_after = min(len(data_frame), index_id + 6)
result_after = data_frame.iloc[start_index_after:end_index_after]

# Преобразование DataFrame в строку с помощью tabulate и сохранение в файл. Кодировка для русского языка.
with open('output.txt', 'w', encoding='utf-8') as f:
    f.write("5 строк до найденной строки:\n")
    f.write(tabulate(result_before, headers=result_before.columns, tablefmt="plain") + '\n\n')

    # Считаю количество столбцов в строке
    num_columns_found = result_found.shape[1]
    f.write("Найденная строка (Количество столбцов в строке: {}):\n".format(num_columns_found))
    f.write(tabulate(result_found, headers=result_found.columns, tablefmt="plain") + '\n\n')


    f.write("5 строк после найденной строки:\n")
    f.write(tabulate(result_after, headers=result_after.columns, tablefmt="plain") + '\n\n')

# Вывод строки
print("Запись завершена. Результаты сохранены в файл 'output.txt'.")
