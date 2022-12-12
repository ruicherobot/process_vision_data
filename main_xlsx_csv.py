# Extracting sum of open hours and total check-out people
import numpy as np
import pandas as pd
import math
import openpyxl
import matplotlib.pyplot as mp

if __name__ == '__main__':

    # importe required libraries
    import openpyxl
    import csv
    import pandas as pd

    # open given workbook
    # and store in excel object
    excel = openpyxl.load_workbook("Data_for_MIKE_0801.xlsx")
    for datename in excel.sheetnames:
        filename = datename + ".csv"
        sheet = excel[datename]
        col = csv.writer(open(filename, 'w', newline=""))
        count = 0
        for r in sheet.rows:
            count = count + 1
            if count < 4:
                continue
            col.writerow([cell.value for cell in r])

        # df = pd.DataFrame(pd.read_csv(filename))