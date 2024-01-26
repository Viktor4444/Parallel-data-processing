import multiprocessing
import pickle
import queue
from time import sleep


def genrator(data_queue):
    for i in range(5):
        sleep(1)
        data_queue.put(i)

        print(f'genrator: genrate {i}')
        
    print(f'genrator: done')


def validate_data(data):
    if type(data) is int:
        return True
    else:
        return False 


def processor(lock, data_queue, file_to_save='./data.pickle'):
    while True:
        try:
            data = data_queue.get(timeout=10.0)
            print(f'processor: {data}')
            is_valid_data = validate_data(data)

            if is_valid_data:
                lock.acquire()
                with open("data.pickle", "rb") as file:
                    file_data = pickle.load(file)

                file_data.append(data)

                with open("data.pickle", "wb") as file:
                    pickle.dump(file_data, file)
                lock.release()

            sleep(0.3)
        except queue.Empty as e:
            print(f'processor Done')
            break


def send_to_imaginary_server(data):
    pass


def sender(lock, load_file='./data.pickle'):
    batch_num = 0
    count_cycles_without_data = 0
    while True:
        sleep(3)
        lock.acquire()
        with open("data.pickle", "rb") as file:
            file_data = pickle.load(file)
        if file_data:
            data_to_send = file_data.copy()
            file_data.clear()
            with open("data.pickle", "wb") as file:
                pickle.dump(file_data, file)
            lock.release() 

            for data in data_to_send:
                print(f"sending {data}...")
                send_to_imaginary_server(data)
                sleep(1)
                print(f"{data} sended!")

            print(f'data batch num {batch_num} sended')
            batch_num += 1
            count_cycles_without_data = 0
        else:
            lock.release()
            count_cycles_without_data += 1

        if count_cycles_without_data == 5:
            break
    print("Sender done")


def main():
    inital_lsit = []
    with open("data.pickle", "wb") as file:
        pickle.dump(inital_lsit, file)

    lock = multiprocessing.Lock()
    data_queue = multiprocessing.Queue()

    p1 = multiprocessing.Process(target=genrator, args=(data_queue,))
    p2 = multiprocessing.Process(target=processor, args=(lock, data_queue,))
    p3 = multiprocessing.Process(target=sender, args=(lock,))

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()


def test_data():
    with open("data.pickle", "rb") as file:
        data = pickle.load(file)
        print(data)


if __name__ == '__main__':
    test_data()
    # main()
