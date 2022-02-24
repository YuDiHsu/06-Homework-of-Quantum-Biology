import random
import os
import multiprocessing as mp
import pygsheets
import xlsxwriter
from functools import partial
from multiprocessing import freeze_support
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import glob
import datetime
import re


def data_set_generator(letter: str, size: int):
    all_digits = set()
    for i in range(10):
        for j in range(10):
            for z in range(10):
                all_digits.add(f"{letter}{i}{j}{z}")
    return set(random.sample(all_digits, size))


def access_google_sheets(path, read_type):
    # # # ------------------ 讀取本地端 ------------------
    # xlsx = pd.ExcelFile(path)
    # sheets_list = xlsx.sheet_names
    # final_cascade_list = []
    # sheet_name_set = set()
    # for sheet in sheets_list:
    #     df = xlsx.parse(sheet, header=None)
    #     df = df.fillna('')
    #     cascade_list = df.values.tolist()
    #     for sig_cas in cascade_list:
    #         new_sig_cas = {}
    #         for signal in sig_cas:
    #             if signal:
    #                 if signal not in new_sig_cas:
    #                     new_sig_cas[signal] = False
    #         if new_sig_cas not in final_cascade_list:
    #             final_cascade_list.append({f"{sheet}": new_sig_cas})
    #             sheet_name_set.add(sheet)
    # # # ------------------ 讀取本地端 ------------------

    # # ------------------ 讀取google sheet ------------------
    final_sh_data_list = []
    sh_name_set = set()
    # gc = pygsheets.authorize(service_account_file=os.path.join('.', 'qantumbiology-2.json'))
    gc = pygsheets.authorize(client_secret=os.path.join('.', 'client_secrets.json'))

    sh = gc.open_by_url(path)
    sh.worksheets()
    for sheet in sh.worksheets():
        ws = sh.worksheet_by_title(sheet.title)
        df = ws.get_as_df(start='A1', index_colum=0, empty_value='', include_tailing_empty=False, has_header=False)
        df = df.fillna('')
        sh_data_list = df.values.tolist()
        # # read data set
        if read_type == 'data_set':
            sh_name_set.add(sheet.title)
            for sh_d in sh_data_list:
                final_sh_data_list += sh_d
        # # read signal pathway cascades
        if read_type == 'signal_pathway':
            for sig_cas in sh_data_list:
                new_sig_cas = {}
                for signal in sig_cas:
                    if signal:
                        if signal not in new_sig_cas:
                            new_sig_cas[signal] = False
                if new_sig_cas not in final_sh_data_list:
                    final_sh_data_list.append({f"{sheet.title}": new_sig_cas})
                    sh_name_set.add(sheet.title)
    # # ------------------ 讀取google sheet ------------------

    return final_sh_data_list, sh_name_set


def process_func(cascade_data_list, data_set):
    final_cascade_data_list = []
    for cascade_data in cascade_data_list:
        for cas_name, sig in cascade_data.items():
            for sig_name in sig.keys():
                if sig_name in data_set:
                    cascade_data[cas_name][sig_name] = True

        for k, v in cascade_data.items():
            verify = set()
            for m, n in v.items():
                verify.add(n)
            if len(verify) == 1 and list(verify)[0] is False:
                break
            else:
                final_cascade_data_list.append(cascade_data)
    # print(final_cascade_data_list)
    return final_cascade_data_list


def calibrate_data_list(data_list, m_l):
    all_p = []
    # # calibrate length
    for dd_ in zip(data_list, m_l):
        p = set()
        for d in dd_[0]:

            if dd_[1] - len(d):
                for i in range(dd_[1] - len(d)):
                    d.insert(-1, '')
                    p.add(d[-1])
            else:
                p.add(d[-1])
        all_p.append(sorted(p, reverse=True))

    # # calculate rank
    for dd in zip(data_list, all_p):
        for d in dd[0]:
            d.append(dd[1].index(d[-1]) + 1)

    return data_list


def write_to_xlsx(sig_data, sh_name, max_len_list, t_set, f_set):
    date = datetime.datetime.now().date()
    workbook = xlsxwriter.Workbook(os.path.join('.', 'exported_data', f"signal_pathway_{date.strftime('%Y%m%d')}.xlsx"))
    col_len_width = []
    for idx, data_ in enumerate(zip(sig_data, sh_name)):
        header_format = workbook.add_format(
            {'align': 'center', 'valign': 'vcenter', 'size': 12, 'color': 'black', 'bold': 4})
        header = []
        header_name_list = []
        for i in range(max_len_list[idx] - 2):
            h = {'header': f"Signal_name_{i + 1}", "format": header_format}
            header.append(h)
            header_name_list.append(f"Signal_name_{i + 1}")
        header += [{'header': 'Probability', 'format': header_format}, {'header': 'Rank', 'format': header_format}]
        header_name_list += ['Probability', 'Rank']
        # print(sheet_name)
        sheet = workbook.add_worksheet(data_[1])
        if len(data_[0]):

            sheet.add_table(0, 0, len(data_[0]), len(data_[0][0]) - 1,
                            {'data': sorted(data_[0], key=lambda x: x[-1]), 'autofilter': True, 'columns': header})

            false_format = workbook.add_format({'align': 'center',
                                                'valign': 'vcenter',
                                                'size': 12,
                                                'color': 'gray',
                                                })
            for f in f_set:
                sheet.conditional_format(0, 0, len(data_[0]), len(data_[0][0]) - 1, {'type': 'text',
                                                                                     'criteria': 'containing',
                                                                                     'value': f,
                                                                                     'format': false_format})
            true_format = workbook.add_format({'align': 'center',
                                               'valign': 'vcenter',
                                               'size': 12,
                                               'color': 'black',
                                               'bold': 4,
                                               'bg_color': 'yellow'
                                               })
            for t in t_set:
                sheet.conditional_format(0, 0, len(data_[0]), len(data_[0][0]) - 1, {'type': 'text',
                                                                                     'criteria': 'containing',
                                                                                     'value': t,
                                                                                     'format': true_format})

            for m in range(len(data_[0]) + 1):
                sheet.set_row(m, 30, cell_format=header_format)

            col_len_width.append([len(j) for j in header_name_list])
            for n, l in enumerate(zip(col_len_width[idx], header_name_list)):
                sheet.set_column(n, n, max(l[0], len(l[1])) * 1.6)
    workbook.close()

    return os.path.join('.', 'exported_data', f"signal_pathway_{date.strftime('%Y%m%d')}.xlsx")


def upload(xlsx_file_path):
    gauth = GoogleAuth()
    # Try to load saved client credentials
    gauth.LoadCredentialsFile(os.path.abspath(os.path.join('.', 'mycreds.txt')))
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()

    # Save the current credentials to a file
    gauth.SaveCredentialsFile(os.path.abspath(os.path.join('.', 'mycreds.txt')))

    drive = GoogleDrive(gauth)
    # # get latest file
    path_list = glob.glob((os.path.abspath(os.path.join('.', 'exported_data', '*.xlsx'))))
    latest_file = sorted(path_list, reverse=True)[0]

    folder_list = drive.ListFile({'q': "'1heqmUz0KeXrALuWOAhd08IHPFVXSbIgk' in parents and trashed=False"}).GetList()

    today = datetime.datetime.now().date()

    file_info_list = []
    if folder_list:
        for gdf in folder_list:
            # # get the info of the file with same name for further replace
            try:
                gdf_file_date = re.search('(20\d{2})(\d{2})(\d{2})', gdf['title']).group()
                if gdf_file_date:
                    gdf_file_date = datetime.datetime.strptime(gdf_file_date, '%Y%m%d').date()
                    if (today - gdf_file_date).days == 0:
                        file_info_list.append(dict(
                            title=gdf['title'], id=gdf['id']))

                    if (today - gdf_file_date).days >= 10:
                        gdf.Delete()

            except Exception as e:
                print(e)

    # # if file name exist, renew file
    if file_info_list:
        for file_info in file_info_list:
            if file_info['title'] == os.path.basename(latest_file):
                file = drive.CreateFile(
                    {'parents': [{'id': '1heqmUz0KeXrALuWOAhd08IHPFVXSbIgk'}], 'title': file_info['title'],
                     'id': file_info['id']})
                file.SetContentFile(latest_file)
                # url = f"https://drive.google.com/thumbnail?id={img_info['id']}&sz=w1920-h1080"
                file.Upload()
                # print(file['title'], url)
                print(file['title'] + '---Renew and Upload success')

    # # upload new file
    else:
        file = drive.CreateFile(
            {'parents': [{'id': '1heqmUz0KeXrALuWOAhd08IHPFVXSbIgk'}], 'title': os.path.basename(xlsx_file_path)})
        file.SetContentFile(xlsx_file_path)
        file.Upload()
        print(file['title'] + '---Upload success')


if __name__ == "__main__":
    freeze_support()
    # cascade_data_list, sheet_name_set = cascade_func(os.path.join('.', 'cascade.xlsx'))

    # # read signal pathway cascades from google sheets
    cascade_data_list, sheet_name_set = access_google_sheets(
        'https://docs.google.com/spreadsheets/d/1nVYaoFC8SusZPkPwe37DdcVECvuEs0yKU1Gka2QF3rE/', 'signal_pathway')

    # # get data set from google sheet
    # data_set = data_set_generator('A', 50)
    # access_google_sheets('https://docs.google.com/spreadsheets/d/1y2pI7y-gCXw9IBfHgXIFANcBdY2WNkNq-bWoCrwK554/',
    #                      'data_set')
    data_set = set(
        access_google_sheets('https://docs.google.com/spreadsheets/d/1y2pI7y-gCXw9IBfHgXIFANcBdY2WNkNq-bWoCrwK554/',
                             'data_set')[0])

    # # multiprocessing
    num_processes = int(mp.cpu_count() / 2)
    chunk_size = int(len(cascade_data_list) / num_processes)
    chunks = [cascade_data_list[i:i + chunk_size] for i in range(0, len(cascade_data_list), chunk_size)]

    pool = mp.Pool(processes=num_processes)
    result_list = pool.map(partial(process_func, data_set=data_set), chunks)

    # result_list = pool.map(process_func, chunks)

    signal_pathway_data_set = []
    for result in result_list:
        if result:
            signal_pathway_data_set += result
    # print(signal_pathway_data_set)
    # # get expanded data and calculate probability
    false_set = set()
    true_set = set()
    m_l = []
    sig_names_data_list = [[], []]
    value_data_list = []
    for sheet_name in sorted(sheet_name_set):
        temp_sig_list = []
        temp_len = set()
        for data_dict in signal_pathway_data_set:
            # print(data_dict)
            if sheet_name in data_dict.keys():
                sig_names = list(data_dict[sheet_name].keys())

                # # calculate probability of signal transduction
                for kk, vv in data_dict.items():
                    for k, v in vv.items():
                        if v is False:
                            false_set.add(k)
                        if v is True:
                            true_set.add(k)
                probability = round(
                    list(data_dict[sheet_name].values()).count(True) / len(data_dict[sheet_name].values()), 1) * 100
                sig_names.append(probability)
                temp_len.add(len(sig_names))
                temp_sig_list.append(sig_names)

                if sheet_name not in sig_names_data_list[1]:
                    sig_names_data_list[1].append(sheet_name)
        # # get expanded data
        if temp_sig_list:
            sig_names_data_list[0].append(temp_sig_list)
        # # get max length of data of every cascade
        if temp_len:
            m_l.append(max(temp_len))

    # # calibrate data length to the same and get rank
    sig_names_data_list[0] = calibrate_data_list(sig_names_data_list[0], m_l)

    new_max_length = [i + 1 for i in m_l]

    # # write to excel
    xlsx_path = write_to_xlsx(sig_names_data_list[0], sig_names_data_list[1], new_max_length, true_set, false_set)

    # # upload to google drive
    upload(xlsx_path)
