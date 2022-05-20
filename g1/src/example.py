"""
This is an example of some things you might want in your code
"""

# Import logging
import logging
logger = logging.getLogger(__name__)

def simple_function(some_int, some_flag=False):
    """
    Insert Useful description here...
    This multiplies an input by -1 if some_flag is true
    :param some_int: an int
    :param some_flag:  a bool
    :return: some_int if some_flag is true, -some_int otherwise
    """

    logging.info('This is a log message')
    logging.debug(f'This is a debug level message. Function called with {input}')
    if some_flag:
        some_int *= -1
    return some_int

import argparse
if __name__ == '__main__':
    # Configure logging to be output
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Use argparse to get some inputs
    # Running `python src/example.py --help` will show the options
    parser = argparse.ArgumentParser(description='This is an example')
    parser.add_argument('--some-flag', action='store_true', help='example of passing in a boolean')
    parser.add_argument('some_int', type=int, help='Example of passing in an int')
    args=parser.parse_args()

    # Run something with those args
    print(simple_function(args.some_int, some_flag=args.some_flag))
