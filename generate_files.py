import os

def create_big_text_file(directory, file_name, size_in_gb, character):

    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Size in GB to bytes (1 GB = 1 * 1024 * 1024 * 1024 bytes)
    size_in_bytes = size_in_gb * 10 * 1024 * 1024

    # Calculate how many characters we need (1 character = 1 byte)
    num_characters = size_in_bytes

    file_path = os.path.join(directory, file_name)

    # Open the file in text mode
    with open(file_path, 'w') as f:
        # Write the specified character multiple times until the file is of the desired size
        f.write(character * num_characters)

def create_multiple_files(directory, num_files, size_in_gb, character='A'):
    for i in range(1, num_files + 1):
        file_name = f"file_{i}.txt"
        print(f"Creating {file_name}")
        create_big_text_file(directory, file_name, size_in_gb, character)
        print(f"{file_name} created.")

if __name__ == "__main__":
    # Specify the number of files and size in GB for each file
    directory = "s3-zip-store/src/test/resources"
    num_files = 100
    size_in_gb = 1
    character = 'A'  # The character to write in the file

    # Create the files
    create_multiple_files(directory, num_files, size_in_gb, character)

