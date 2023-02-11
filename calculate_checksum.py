import os
import hashlib

'''
Narzedzie do liczenia checksumy, w szczegolnosci przydatne
przy crawlowaniu, gdzie spotykamy sie z duza iloscia
duplikatow typu przekierowania do glownej domeny

sugeruje przy crawlowaniu zapisywac artykuly do plikow txt,
-walidowac, usuwac duplikaty, usuwac zle formatowania
dopiero na czystym txt budowac .zst
'''
#przy pierwszym uzyciu stworzyc pusty plik checksum.txt
with open("checksum.txt", 'r') as f:
    files_done = f.read().split("\n")

folders = os.listdir("dataset_path/")

for c, i in enumerate(folders):
    try:
        with open("dataset_path/"+i, "rb") as file:
            result = hashlib.md5(file.read())
        if c % 1000 == 0:
            print(c, i, result.hexdigest(), len(files_done))
        if result.hexdigest() in files_done:
            print("----DUPLICATE ", i)
            os.remove("dataset_path/"+i)
        else:
            files_done.append(result.hexdigest())
            with open("checksum.txt", 'a') as f:
                f.write(result.hexdigest()+"\n")
    except Exception as e:
        print(e)
        pass
