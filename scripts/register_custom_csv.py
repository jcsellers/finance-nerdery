from zipline.data.bundles import register
from zipline.data.bundles.csvdir import csvdir_equities

def register_custom_csv():
    register(
        'custom_csv',
        csvdir_equities(['daily'], '/path/to/your/csvdir'),
        calendar_name='NYSE',  # Adjust as needed
    )

if __name__ == "__main__":
    register_custom_csv()
