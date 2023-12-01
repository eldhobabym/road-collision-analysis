import os


def lookup_data_gen(data):
    data_list = [(key, value) for key, value in data.items()]
    return data_list


def get_file_names(directory_path):
    try:
        # Get the list of files in the specified directory
        filenames = os.listdir(directory_path)

        # Print the list of filenames

        return filenames

    except OSError:
        print(f"Error reading directory: {directory_path}")
        return None
