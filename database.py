import csv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, text as sa_text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from dateutil.parser import parse

# Подключаемся к базе данных PostgreSQL
DATABASE_URL = "postgresql://postgres:sa@localhost:5431/police"

# Путь к CSV файлу
CSV_FILE_PATH = r'C:\Users\hqmnd\Desktop\police-department-calls-for-service.csv'

# Определение базовой модели (базового класса)
base = declarative_base()

# Определение модели (класса) 
class YourModel(base):
    __tablename__ = 'calls'
    id = Column(Integer, primary_key=True)
    crime_type_name = Column(String)
    call_date_time = Column(DateTime)
    disposition = Column(String)
    address = Column(String)
    city = Column(String, nullable=True)
    state = Column(String)
    agency_id = Column(Integer)
    address_type = Column(String)
    common_location = Column(String)

def main():
    # Подключаемся к базе данных
    engine = create_engine(DATABASE_URL, echo=True)  # `echo=True` печатает SQL-запросы при выполнении
    base.metadata.create_all(engine)  # Создаем таблицу 
    session = Session(engine)  # Создаем объект сессии для работы с базой данных

    session.execute(sa_text('''TRUNCATE TABLE calls;'''))  # Очищаем таблицу от старых данных
    session.commit()

    batch_size = 200000  # Размер пакета, количество строк для загрузки за одну итерацию
    buffer = []  # Буфер для хранения данных перед загрузкой


    # Открываем CSV файл и загружаем данные в базу данных
    with open(CSV_FILE_PATH, 'r') as csv_file:
        reader = csv.reader(csv_file)
        next(reader)  # Пропускаем заголовок CSV файла (если есть)
        
        #row_number = 0

        for row in reader:
            try:
                while len(row) < 10:
                    row.append(None)

                # Разбираем данные из строки CSV, так же с помощью среза убрал лишнии данные по типу времени и оставил "call_date_time"
                entry_id, crime_type_name, call_date_time, \
                    disposition, address, city, state, agency_id, address_type, common_location, *fields = row[:2] + row[6:]

                # Преобразование данных, если необходимо
                entry_id = int(entry_id)
                agency_id = int(agency_id)
                #Смотрим строчку. Если там строка текста оставляем без пробелов, 
                # если ничего нет присваиваем None
                if city.strip():
                    city = city.strip()
                else:
                    city = None


                # Создаем экземпляр модели и присваиваем значения столбцам
                instance = YourModel(
                    id = entry_id,
                    crime_type_name = crime_type_name.strip(),
                    call_date_time = call_date_time.strip(),
                    disposition = disposition.strip(),
                    address = address.strip(),
                    city = city,
                    state = state.strip(),
                    agency_id = agency_id,
                    address_type = address_type.strip(),
                    common_location = common_location.strip(),
                )
                buffer.append(instance)


                # Загружаем данные пакетом по batch_size
                if len(buffer) >= batch_size:
                    session.add_all(buffer)
                    session.commit()
                    buffer = []


            #Если возникает ошибка печатаем ее в дебагере и выходим
            except Exception as e:
                print(e)
                exit(1)


        # Если остались несохраненные данные, сохраняем их
        if buffer:
            session.add_all(buffer)
            session.commit()


if __name__ == "__main__":
    main()