from tkinter import Tk, Label, Button
from tkinter.filedialog import askopenfilename

import pandas as pd

import zipfile
import os

window = Tk()
window.geometry("500x500")
window.title("CSV file processing")

dir_path ="" 
file_name = ""

y = 60.0

def display_step(step):
    global y
    label = Label( window, text = step)
    label.place(x = 10.0, y = y, anchor = 'sw')
    label.update()
    y+=20

def load_file(input_file_path):
    display_step("Processing started")
    file_name_path, file_extension = os.path.splitext(input_file_path)
    try:
        if file_extension == '.zip':
            with zipfile.ZipFile(input_file_path) as zip_ref:
                display_step("Unzipping the file...")
                zip_ref.extractall(dir_path)
        df = pd.concat(chunk for chunk in pd.read_csv(file_name_path+".csv", 
                                                      chunksize=10**5,
                                                      names=list(range(3))))
        if file_extension not in('.csv', '.zip'):
            display_step('Invalid file loaded')
            return 0
    except FileNotFoundError as fnf:
        display_step(fnf)
    display_step('File loaded successfully for processing')
    data_segregation(df)

def data_segregation(df):
    display_step("Data loaded : "+str(len(df))+" rows")
    #Erred data loaded
    error_data = df[df[1].notnull()]
    if len(error_data) > 0:
        display_step('Erred data found in file')
    else:
        display_step('No erred data found')
	#Row indexes with erred data present
    error_idx_list = error_data.index.values.tolist()
    
	#Errer data removed, only correct data loaded
    correct_df_before_dedup = df.drop(df.index[error_idx_list])
	#Remove duplicates from correct data
    display_step('Correct data Row count before deduplication : '+str(len(correct_df_before_dedup)))
    correct_df_after_dedup = correct_df_before_dedup.drop_duplicates()
    correct_df_after_dedup = correct_df_after_dedup[[0]]
    if len(correct_df_before_dedup) != len(correct_df_after_dedup):
        display_step('Correct data Row count after deduplication : '+str(len(correct_df_after_dedup)))
    else:
        display_step('No duplicates found in correct data rows')
	#Correcting the erred data
    error_data_before_dedup = error_data[error_data.columns[:]].apply(lambda x: 't'.join(x.dropna().astype(str)), axis = 1)
	#Remove duplicate data from corrected erred data
    display_step('Erred data Row count before deduplication : '+str(len(error_data_before_dedup)))
    error_data_after_dedup = error_data_before_dedup.drop_duplicates()
    if len(error_data_before_dedup) != len(error_data_after_dedup):
        display_step('Correct data Row count after deduplication : '+str(len(error_data_after_dedup)))
    else:
        display_step('No duplicates found in erred data rows') 
    #Combine correct data and erred data (duplicates removed)
    final_data_to_csv = pd.concat([correct_df_after_dedup, error_data_after_dedup])
    save_data_to_file(final_data_to_csv)

def save_data_to_file(final_data_to_csv):
	#Save data to file
    display_step('Data saving to file in progress...')
    if not os.path.exists(dir_path+"/processed files/"):
        os.makedirs(dir_path+"/processed files/")
    final_data_to_csv.to_csv(dir_path+"/processed files/"+file_name+".csv", 
							 header=False, 
							 index = False)
    display_step('Final correct file saved successfully : '+str(len(final_data_to_csv)))
	
	
def get_path():
    global dir_path
    global file_name
    input_file_path = askopenfilename()
    dir_path = os.path.dirname(input_file_path)
    base=os.path.basename(input_file_path)
    file_name = os.path.splitext(base)[0]
    load_file(input_file_path)
    

btn = Button(window, text = 'Upload File', command = lambda:get_path())
btn.pack()

def close_window(): 
    os.remove(dir_path+"/"+file_name+".csv")
    window.destroy()

cls_btn = Button(window, text = "Close", command = lambda:close_window())
cls_btn.place(x = 230, y = 450)

window.mainloop()
