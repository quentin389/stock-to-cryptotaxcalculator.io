import argparse

from parsers.EtoroImport import EtoroImport

parser = argparse.ArgumentParser("etoro-import.py")

parser.add_argument(
    'source',
    help='Source file with the eToro data to be changed to cryptotaxcalculator.io "Advanced Manual CSV" format. '
         'This source file has to be generated using the "xls" button on eToro "Account statement" page.',
    type=str
)
parser.add_argument(
    'target',
    help='Target file name to save the data to. Skip the extension, it will always be ".cvs". The file will be '
         'created in the current directory and will overwrite any existing file. ',
    type=str
)

args = parser.parse_args()

etoroImport = EtoroImport(args.source, args.target)
etoroImport.run()
