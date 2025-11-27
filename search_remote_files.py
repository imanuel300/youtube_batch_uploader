import os

OUTPUT_FILE = 'all_mp4_files.txt'
ROOT_DIR = '.'

mp4_files = []
for root, dirs, files in os.walk(ROOT_DIR):
    for file in files:
        if file.endswith('.mp4'):
            full_path = os.path.join(root, file)
            mp4_files.append(full_path)

with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    for path in mp4_files:
        f.write(path + '\n')

print(f'נמצאו {len(mp4_files)} קבצי mp4. הנתיבים נשמרו בקובץ {OUTPUT_FILE}')
