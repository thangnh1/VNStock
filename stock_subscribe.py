from vnstock import listing_companies
import csv

list = ['VNINDEX', 'VN30', 'HNXIndex', 'HNX30', 'UpcomIndex', 'VNXALL',
        'VN100','VNALL', 'VNCOND', 'VNCONS','VNDIAMOND', 'VNENE', 'VNFIN',
        'VNFINLEAD', 'VNFINSELECT', 'VNHEAL', 'VNIND', 'VNIT', 'VNMAT',
        'VNMID', 'VNREAL', 'VNSI', 'VNSML', 'VNUTI', 'VNX50']


# Function to check if the selected ticker code exists or not.
def check_ticker(ticker):
    with open('subscribe.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if ticker in row:
                return True
    return False


# Function to add a new ticker to the subscribe list.
def new_ticker():
    ticker = input('Please enter your ticker code : ')
    if ticker in list:
        if check_ticker(ticker):
            print('Your ticker code does exist!')
        else:
            with open('subscribe.csv', 'a') as file:
                file.write(ticker + '\n')
            print('{} added!'.format(ticker))
    else:
        print('Your ticker code is invalid.')


# Function to view the list of subscribed tickers.
def view_ticker():
    try:
        with open('subscribe.csv', 'r') as file:
            reader = csv.reader(file)
            rows = [row for row in reader]

            if not rows:
                print("Empty.")
            else:
                print('List your ticker code : ')
                for i, row in enumerate(rows, 1):
                    print(f'{i}. {row[0]}')  # Hiển thị mã code
    except:
        print('View error!')


# Function to remove a ticker that is no longer wanted to be subscribed.
def delete_ticker():
    ticker = input('Please enter ticker code you want to delete : ')
    with open('subscribe.csv', 'r') as file:
        reader = csv.reader(file)
        rows = [row for row in reader]

    if not rows:
        print("Empty.")
        return

    updated_rows = []
    removed = False

    for row in rows:
        if row and row[0] != ticker:
            updated_rows.append(row)
        elif row and row[0] == ticker:
            removed = True

    if not removed:
        print("Your ticker code does not exist.")
        return

    with open('subscribe.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(updated_rows)
    print('{} deleted!'.format(ticker))


# Menu Function
def menu():
    while True:
        print("\n------ MENU SUBCRIBE ------")
        print("1. Add new ticker code")
        print("2. View list ticker code")
        print("3. Delete ticker code")
        print("4. Quit")

        choice = input("Please enter your option (1-4): ")

        if choice == "1":
            new_ticker()
        elif choice == "2":
            view_ticker()
        elif choice == "3":
            delete_ticker()
        elif choice == "4":
            print("Good bye!")
            break
        else:
            print("Your choice is invalid.")


menu()