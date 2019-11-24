def print_black(text):
    print('\033[30m', text, '\033[0m', sep='')


def print_red(text):
    print('\033[31m', text, '\033[0m', sep='')


def print_green(text):
    print('\033[32m', text, '\033[0m', sep='')


def print_yellow(text):
    print('\033[33m', text, '\033[0m', sep='')


def print_blue(text):
    print('\033[34m', text, '\033[0m', sep='')


def print_magenta(text):
    print('\033[35m', text, '\033[0m', sep='')


def print_cyan(text):
    print('\033[36m', text, '\033[0m', sep='')


def print_gray(text):
    print('\033[90m', text, '\033[0m', sep='')


def report_hook(block_number, block_size, total_size):
    progress = block_number * block_size
    if total_size > 0:
        percent = progress * 1e2 / total_size  # 1e2 ==> 1 * 10^2 ==> 100
        s = "\r%5.1f%% %*d / %d" % (percent, len(str(total_size)), progress, total_size)
        print_yellow(s)
