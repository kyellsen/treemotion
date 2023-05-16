import subprocess

def generate_file_structure_md(repo_path):
    # Git-Befehl, um die Dateistruktur anzuzeigen
    cmd = ['git', 'ls-tree', '--name-only', '-r', 'HEAD']

    # Ausführen des Befehls und Erfassen der Ausgabe
    result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True)
    output = result.stdout.strip()

    # Formatieren der Ausgabe in Markdown
    file_structure_md = ""
    for item in output.split('\n'):
        if '/' in item:
            indent = "    " * item.count('/')
            file_structure_md += f"{indent}- {item}\n"

    return file_structure_md

# Pfad zum Git-Repository
repo_path = '/Pfad/zum/Repository'

# Generiere die Markdown-Dateistruktur
file_structure_md = generate_file_structure_md(repo_path)

# Datei speichern
output_file = 'file_structure.md'
with open(output_file, 'w') as file:
    file.write(file_structure_md)

print(f"Die Datei '{output_file}' wurde erfolgreich erstellt.")
