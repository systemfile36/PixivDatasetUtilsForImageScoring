import os

def fix_png_filenames(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith("png") and not filename.endswith(".png"):
                old_path = os.path.join(dirpath, filename)
                new_filename = filename[:-3] + ".png"
                new_path = os.path.join(dirpath, new_filename)

                # 이름이 충돌하지 않을 경우에만 변경
                if not os.path.exists(new_path):
                    os.rename(old_path, new_path)
                    print(f"Renamed: {old_path} -> {new_path}")
                else:
                    print(f"Skipped (already exists): {new_path}")

if __name__ == "__main__":
    fix_png_filenames("H:/CharacterTags/images")

    
