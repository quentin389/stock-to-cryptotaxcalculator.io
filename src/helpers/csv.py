import csv

from config.types import OutputRow


def save_output(target_file: str, data: list[OutputRow]) -> None:
    file_name = f'{target_file}.csv'
    with open(file_name, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(('Timestamp (UTC)', 'Type', 'Base Currency', 'Base Amount', 'Quote Currency (Optional)',
                         'Quote Amount (Optional)', 'Fee Currency (Optional)', 'Fee Amount (Optional)',
                         'From (Optional)', 'To (Optional)', 'ID (Optional)', 'Description (Optional)'))
        writer.writerows([(row.TimestampUTC, row.Type, row.BaseCurrency, row.BaseAmount, row.QuoteCurrency,
                           row.QuoteAmount, row.FeeCurrency, row.FeeAmount,
                           row.From, row.To, row.ID, row.Description)
                          for row in data])
        print(f'\nOutput file saved as {file_name}. '
              f'This should import {len(data)} transactions to cryptotaxcalculator.io.')
