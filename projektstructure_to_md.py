import os

def generate_file_structure_md(root_dir):
    file_structure = ""
    for root, dirs, files in os.walk(root_dir):
        level = root.replace(root_dir, "").count(os.sep)
        indent = "    " * (level)
        file_structure += f"{indent}- {os.path.basename(root)}/\n"
        sub_indent = "    " * (level + 1)
        for file in files:
            file_structure += f"{sub_indent}- {file}\n"

    return file_structure

current_dir = os.path.dirname(os.path.abspath(__file__))
file_structure_md = generate_file_structure_md(current_dir)

with open("file_structure.md", "w") as file:
    file.write(file_structure_md)
