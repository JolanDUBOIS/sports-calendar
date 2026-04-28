class TemporaryConsolePrinter:
    def __init__(self) -> None:
        self.max_len = 0

    def print(self, msg: str) -> None:
        self.max_len = max(self.max_len, len(msg))
        print(f"\r{msg.ljust(self.max_len)}", end='', flush=True)

    def clear(self) -> None:
        print('\r' + ' ' * self.max_len + '\r', end='', flush=True)
