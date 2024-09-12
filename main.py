from analyzer import contract_analyzer
from config import PAIR_ADDRESS, START_TIME, END_TIME, ACTION


def main():
    contract_analyzer(PAIR_ADDRESS, START_TIME, END_TIME, ACTION.lower())


if __name__ == '__main__':
    main()
