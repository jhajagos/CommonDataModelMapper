import glob
import os
import argparse


def main(directory, glob_pattern, combined_file_name):
    glob_results = glob.glob(os.path.join(directory, glob_pattern))
    files_to_combine = [os.path.split(gr)[-1] for gr in glob_results]

    files_to_combine.sort()

    combined_file_name_path = os.path.join(directory, combined_file_name)

    with open(combined_file_name_path, "w", newline="") as fw:
        i = 0
        for part_file_name in files_to_combine:

            part_file_name_path = os.path.join(directory, part_file_name)

            print("Reading '%s'" % part_file_name_path)

            with open(part_file_name_path, "rb") as f:
                if i > 0:
                    f.__next__()  # skip the header

                for line in f:
                    fw.write(line)

            i += 1


if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser()
    arg_parser_obj.add_argument("-d", "--directory", dest="directory",
                                help="Directory")
    arg_parser_obj.add_argument("-s", "--search-pattern", dest="search_pattern", help="Search pattern for files 'files_*.csv'")
    arg_parser_obj.add_argument("-o", "--out-filename", dest="outfile_name",
                                help="Combined filename")

    arg_obj = arg_parser_obj.parse_args()
    main(arg_obj.directory, arg_obj.search_pattern, arg_obj.outfile_name)
