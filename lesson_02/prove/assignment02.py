"""
Course    : CSE 351
Assignment: 02
Student   : <your name here>

Instructions:
    - review instructions in the course
"""

# Don't import any other packages for this assignment
import os
import random
import threading
from money import *
from cse351 import *

# ---------------------------------------------------------------------------
def main(): 

    print('\nATM Processing Program:')
    print('=======================\n')

    create_data_files_if_needed()

    # Load ATM data files
    data_files = get_filenames('data_files')
    
    log = Log(show_terminal=True)
    log.start_timer()

    bank = Bank()

    # Create and start one thread per ATM file
    readers = []
    for file in data_files:
        reader = ATM_Reader(file, bank)
        readers.append(reader)
        reader.start()

    # Wait for all ATM readers to finish
    for reader in readers:
        reader.join()

    test_balances(bank)

    log.stop_timer('Total time')


# ===========================================================================
class ATM_Reader(threading.Thread):
    """Reads ATM transactions from one file in a separate thread."""
    def __init__(self, filename:str, bank:'Bank'):
        super().__init__()
        self._filename = filename
        self._bank = bank

    def run(self):
        with open(self._filename, 'r') as file:
            for line in file:
                line = line.strip()
                if line.startswith('#') or line.strip() == '':
                    continue
                account_num, trans_type, amount_str = line.strip().split(',')
                account_num = int(account_num)
                amount = Money(amount_str)

                if trans_type == 'd':
                    self._bank.deposit(account_num, amount)
                elif trans_type == 'w':
                    self._bank.withdraw(account_num, amount)
                else:
                    print(f'aaaaaaahhhhhhh: {trans_type}')


# ===========================================================================
class Account:
    """Represents a bank account with thread-safe operations."""
    def __init__(self): 
        self.balance = Money('0')
        self.lock = threading.Lock()

    def deposit(self, amount:Money):
        with self.lock:
            self.balance.add(amount)

    def withdraw(self, amount:Money): 
        with self.lock:
            self.balance.sub(amount)
    def get_balance(self) -> Money:
        with self.lock:
            return self.balance


# ===========================================================================
class Bank:
    """Manages multiple accounts and provides thread-safe operations."""
    def __init__(self):
        # Accounts 1..20
        self.accounts = {i: Account() for i in range(1, 21)}
        self.bank_lock = threading.Lock()

    def deposit(self, account_number:int, amount:Money):
        #self._look_Up_Account(account_number).deposit(amount)
        self.accounts[account_number].deposit(amount)

    def withdraw(self, account_number:int, amount:Money):
        #self._look_Up_Account(account_number).withdraw(amount)
        self.accounts[account_number].withdraw(amount)

    def get_balance(self, account_number:int) -> Money:
        #return self._look_Up_Account(account_number).get_balance()
        return self.accounts[account_number].get_balance()
    
    def _look_Up_Account(self, account_number:int) -> Account:
        if account_number in self.accounts:
            return self.accounts[account_number]
        with self.bank_lock:
            if account_number not in self.accounts:
                self.accounts[account_number] = Account()
            self.bank_lock.release()
        return self.accounts[account_number]


# ---------------------------------------------------------------------------

def get_filenames(folder):
    """ Don't Change """
    filenames = []
    for filename in os.listdir(folder):
        if filename.endswith(".dat"):
            filenames.append(os.path.join(folder, filename))
    return filenames

# ---------------------------------------------------------------------------
def create_data_files_if_needed():
    """ Don't Change """
    ATMS = 10
    ACCOUNTS = 20
    TRANSACTIONS = 250000

    sub_dir = 'data_files'
    if os.path.exists(sub_dir):
        return

    print('Creating Data Files: (Only runs once)')
    os.makedirs(sub_dir)

    random.seed(102030)
    mean = 100.00
    std_dev = 50.00

    for atm in range(1, ATMS + 1):
        filename = f'{sub_dir}/atm-{atm:02d}.dat'
        print(f'- {filename}')
        with open(filename, 'w') as f:
            f.write(f'# Atm transactions from machine {atm:02d}\n')
            f.write('# format: account number, type, amount\n')

            # create random transactions
            for i in range(TRANSACTIONS):
                account = random.randint(1, ACCOUNTS)
                trans_type = 'd' if random.randint(0, 1) == 0 else 'w'
                amount = f'{(random.gauss(mean, std_dev)):0.2f}'
                f.write(f'{account},{trans_type},{amount}\n')

    print()

# ---------------------------------------------------------------------------
def test_balances(bank):
    """ Don't Change """

    # Verify balances for each account
    correct_results = (
        (1, '59362.93'),
        (2, '11988.60'),
        (3, '35982.34'),
        (4, '-22474.29'),
        (5, '11998.99'),
        (6, '-42110.72'),
        (7, '-3038.78'),
        (8, '18118.83'),
        (9, '35529.50'),
        (10, '2722.01'),
        (11, '11194.88'),
        (12, '-37512.97'),
        (13, '-21252.47'),
        (14, '41287.06'),
        (15, '7766.52'),
        (16, '-26820.11'),
        (17, '15792.78'),
        (18, '-12626.83'),
        (19, '-59303.54'),
        (20, '-47460.38'),
    )

    wrong = False
    for account_number, balance in correct_results:
        bal = bank.get_balance(account_number)
        print(f'{account_number:02d}: balance = {bal}')
        if Money(balance) != bal:
            wrong = True
            print(f'Wrong Balance: account = {account_number}, expected = {balance}, actual = {bal}')

    if not wrong:
        print('\nAll account balances are correct')


if __name__ == "__main__":
    main()