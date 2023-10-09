from time import gmtime, strftime


def log(msg: str):
    time = strftime("%H:%M:%S", gmtime())
    print(f"[{time}] {msg}")