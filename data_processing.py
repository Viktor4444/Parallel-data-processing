import argparse
import multiprocessing
import pickle

from basic_classes import GeneratorData, ProcessorData, SenderData


def parse_args() -> argparse.ArgumentParser:
    """Function to parse arguments from command line

    Returns:
        argparse.ArgumentParser: arguments
    """
    parser = argparse.ArgumentParser(
        description='Script that simulates parallel '
        'computing, processing, and sending data'
        )
    parser.add_argument(
        '-c', '--count',
        type=int,
        required=True,
        help='how much data needs to be generated'
        )
    parser.add_argument(
        '-s', '--storage',
        type=str,
        default="./data.pickle",
        help='path to file where the data is stored'
        )
    parser.add_argument(
        '-a', '--address',
        type=str,
        default="img.serv.com",
        help='address of the server to send'
        )

    # TODO: сделать повторяемость эксперимента
    # изменяемым параметром.

    # parser.add_argument(
    #   '--set_seed',
    #   action='store_true',
    #   help='Specify this argument if you want '
    #   'that the experiment was be repeatable'
    #   )

    return parser.parse_args()


def run_data_pipeline():
    """running three parallel processes to
    generate, validate, and send data
    """
    # парсим аргументы их командной строки
    args = parse_args()
    # инициализируем необходимые для работы объекты

    # имитация инициализации/подключения к хранилищу
    try:
        inital_lsit=[]
        with open(args.storage, "wb") as file:
            pickle.dump(inital_lsit, file)
    except IOError as error:
        raise IOError(
            'Storage file was not found, please check path to storage.'
            )

    # чтобы одновременно не изменять один и тот же файл нужен лок
    lock = multiprocessing.Lock()
    # для обмена данные между процессами - очередь
    data_queue = multiprocessing.Queue()
    # и флаг для завершения работы программы
    generation_is_ending = multiprocessing.Event()

    # создаём объекты классов Генератор, Обработчик и Отправитель
    d_generator = GeneratorData(data_queue, args.count)
    d_processor = ProcessorData(data_queue, args.storage)
    d_sender = SenderData(args.storage, args.address)

    # и запускаем 3 модуля программы паралелльно
    p_generator = multiprocessing.Process(
        target=d_generator.start_genertion,
        args=(generation_is_ending,)
        )
    p_processor = multiprocessing.Process(
        target=d_processor.run_processing,
        args=(lock, generation_is_ending,)
        )
    p_sender = multiprocessing.Process(
        target=d_sender.run_sending,
        args=(lock, generation_is_ending,)
        )

    p_generator.start()
    p_processor.start()
    p_sender.start()

    p_generator.join()
    p_processor.join()
    p_sender.join()


if __name__ == '__main__':
    run_data_pipeline()
