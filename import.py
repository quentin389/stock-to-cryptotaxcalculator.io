import argparse

from src.parsers.AbstractDataParser import AbstractDataParser

argument_parser = argparse.ArgumentParser("import.py")

argument_parser.add_argument(
    'parser',
    help='The parser to user.',
    choices=['etoro', 'ibkr', 'ig-shares', 'ig-cfd']
)
argument_parser.add_argument(
    'source',
    help='Source file with the data to be changed to cryptotaxcalculator.io "Advanced Manual CSV" format. '
         'See README.md for more information',
    type=str
)
argument_parser.add_argument(
    'target',
    help='Target file name to save the data to. Skip the extension, it will always be ".cvs". The file will be created '
         'in the current directory (or a subdirectory, if you specify it) and will overwrite any existing file. ',
    type=str
)
argument_parser.add_argument(
    '--second_source',
    help='Some formats may require more than one source file.',
    type=str
)

arguments = argument_parser.parse_args()

data_parser: AbstractDataParser
if arguments.parser == 'etoro':
    from src.parsers.etoro.EtoroDataParser import EtoroDataParser
    data_parser = EtoroDataParser(arguments.source, arguments.target)
elif arguments.parser == 'ibkr':
    from src.parsers.ibkr.IbkrDataParser import IbkrDataParser
    data_parser = IbkrDataParser(arguments.source, arguments.target)
elif arguments.parser == 'ig-shares':
    if not arguments.second_source:
        raise Exception('IG Shares format requires "--second_source" to be specified (see README.md)')
    from src.parsers.ig.IgSharesDataParser import IgSharesDataParser
    data_parser = IgSharesDataParser(arguments.source, arguments.second_source, arguments.target)
elif arguments.parser == 'ig-cfd':
    from src.parsers.ig.IgCfdDataParser import IgCfdDataParser
    data_parser = IgCfdDataParser(arguments.source, arguments.target)
else:
    raise Exception("incorrect parser.")

data_parser.run()
