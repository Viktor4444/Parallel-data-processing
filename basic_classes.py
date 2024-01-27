import multiprocessing
import pickle
import queue
from datetime import datetime
from time import sleep

from dateutil.relativedelta import relativedelta
from faker import Faker
from faker.providers import profile

Faker.seed(42)


class GeneratorData():

    """Class for generate fake data

    Attributes:
        count_data (int): the amount of data generated
        queue (multiprocessing.Queue): the queue where the data will be sent
    """

    def __init__(self,
        data_queue: multiprocessing.Queue,
        count_generated_data: int
        ):
        """Writes arguments to the fields (see the description above)

        Args:
            data_queue (multiprocessing.Queue)
            count_generated_data (int)
        """
        self.queue = data_queue
        self.count_data = count_generated_data

    def start_genertion(self,
        generation_is_ending: multiprocessing.Event,
        sleep_time: float = 5
        ):
        """Start generate fake data and send to queue

        Args:
            generation_is_ending (multiprocessing.Event): flag
                end of generation
            sleep_time (float, optional): at what interval
                is the data generated in seconds
        """
        print("Start generating...")
        # создаём класс фейкера
        fake = Faker()
        # и отправляем фейковые данные каждые sleep_time секунд в очередь
        for i in range(self.count_data):
            data = fake.profile()
            self.queue.put(data)
            #(имитация задержки)
            sleep(sleep_time)

        # ставим флаг окончания генерации чтобы остальные процессы завершились
        generation_is_ending.set()
        print(f'Generator done')


class ProcessorData():

    """Class for processed and validate data
    
    Attributes:
        queue (multiprocessing.Queue): the queue from which data will be received
        storage (str): where preocessed data will be saved
    """

    def __init__(self,
        data_queue: multiprocessing.Queue,
        storage_file: str
        ):
        """Writes arguments to the fields (see the description above)

        Args:
            data_queue (multiprocessing.Queue)
            storage_file (str)
        """
        self.queue = data_queue
        self.storage = storage_file

    @staticmethod
    def validate_data(data: profile) -> bool:
        """Validation of data according to the rule "between 30 and 40 years"

        Args:
            data (profile): dict with profile data from Faker

        Returns:
            int|bool: year if data is valid, else False
        """
        birthdate = data["birthdate"]
        current_age = relativedelta(datetime.now(), birthdate)
        if 30 <= current_age.years < 41:
            return current_age.years
        else:
            return False

    def run_processing(self,
        lock: multiprocessing.Lock,
        generation_is_ending: multiprocessing.Event,
        sleep_time: float = 0.3
        ):
        """Start process fake data: receive from queue,
            validate and save in storage if necessary

        Args:
            lock (multiprocessing.Lock): process locker
                for sequential access to storage
            generation_is_ending (multiprocessing.Event): flag
                end of generation
            sleep_time (float, optional): at what interval
                is the data processed in seconds
        """
        print("Start processing...")
        # запускаем бесконечный цикл по типу работы кроны/демона
        while True:
            try:
                # читаем сообщение из очереди
                data = self.queue.get(timeout=30.0)

                # и проверяем данные на необходимые нам условия
                is_valid_data = self.validate_data(data)

                # если данные попадают под условия - отправляем в хранилище
                if is_valid_data:
                    # только один процес может изменять файл в одно время
                    lock.acquire()

                    with open(self.storage, "rb") as file:
                        file_data = pickle.load(file)

                    data["age"] = is_valid_data
                    file_data.append(data)

                    with open(self.storage, "wb") as file:
                        pickle.dump(file_data, file)

                    lock.release()

                # т.к. это обработчик данных - то спим минимально
                # чтобы данные не лежали в очереди
                sleep(sleep_time)
            # если очередь пустая слишком долго - выводим сообщение
            # возможно, генератор сломался, а не закончил работу
            # и стоит проверить что происходит
            except queue.Empty as e:
                print(f'Queue if empty for too long.')

            if generation_is_ending.is_set():
                print(f'Processor done')
                break


class SenderData():

    """Class for read data from storage, clear it, and send data to server

    Attributes:
        address (str): where data will be send
        storage (str): where preocessed data storaged
    """

    def __init__(self,
        storage_file: str,
        server_address: str
        ):
        """Writes arguments to the fields (see the description above)

        Args:
            storage_file (str)
            server_address (str)
        """
        self.storage = storage_file
        self.address = server_address

    def send_to_imaginary_server(self, data: profile):
        """imitate of send data to server with a delay of sending

        Args:
            data (profile): sended data
        """
        self.address
        # имитируем задержку отправки данных
        sleep(0.5)
        print("...")
        sleep(0.5)
        pass

    def run_sending(self,
        lock: multiprocessing.Lock,
        generation_is_ending: multiprocessing.Event,
        sleep_time: float = 10
        ):
        """Start send fake data: collect from storage,
            cleat storage, and send data to server

        Args:
            lock (multiprocessing.Lock): process locker
                for sequential access to storage
            generation_is_ending (multiprocessing.Event): flag
                end of generation
            sleep_time (float, optional): at what interval
                is the data collect and sended in seconds
        """
        print("Start sending...")
        batch_num = 0
        while True:
            sleep(sleep_time)
            # блокируем доступ к данным и читаем
            lock.acquire()
            with open(self.storage, "rb") as file:
                file_data = pickle.load(file)
            # если хранилище не пусто
            if file_data:
                # то сначала очищаем его и снимаем блок
                data_to_send = file_data.copy()
                file_data.clear()
                with open(self.storage, "wb") as file:
                    pickle.dump(file_data, file)
                lock.release()

                # а потом отправляем данные на сервер
                for data in data_to_send:
                    print(f"sending {data["name"]}...")
                    self.send_to_imaginary_server(data)
                    print(f"{data["name"]} sended!")

                    # выводим необходимую ниформацию
                    print("INFO:")
                    [print(f'{key}: {data[key]}') for key in [
                        "name",
                        "sex",
                        "job",
                        "age"
                    ]]
                    print()

                # print(f'data batch num {batch_num} sended')
                batch_num += 1
            else:
                # если хранилище пустое - то снимаем блок
                lock.release()

            if generation_is_ending.is_set():
                print("Sender done")
                break
