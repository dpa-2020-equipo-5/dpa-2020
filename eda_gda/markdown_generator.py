#Autor: Gerardo Mathus - @gemathus
#Autor: Karla Alfaro - @alpika
#oct 2019
from tabulate import tabulate
import time
import datetime

def crate_table_element(tbl, show_index='never'):
    return tabulate(tbl, tablefmt="github", headers='keys', showindex=show_index)

def create_header_element(h_type, text):
    elem = "#" * int(h_type.replace('h', ''))
    return elem + " {}".format(text)

def create_unordered_list_element(lst):
    md_text = ""
    for item in lst:
        md_text += "- {}\n".format(str(item))
    return md_text

def create_image_element(img_sruct):
    title = img_sruct[0]
    path = img_sruct[1].replace('docs/', '')
    return "\n![{}]({})".format(title, path) 

class MarkdownWriter:
    def __init__(self, file_name):
        ts = time.time()
        self.file_name = file_name.lower().replace(' ','_') + datetime.datetime.fromtimestamp(ts).strftime('%Y_%m_%d_%H_%M_%S') + ".md"
        self.content = ""
        self.path = 'docs/' + self.file_name

    def save(self):
        with open(self.path, 'w') as f:
            f.write(self.content)

    def append_to_content(self, md_element, prepend_linebreak=True):
        if prepend_linebreak:
            self.content += "\n{}\n".format(md_element)
        else:
            self.content += "{}\n".format(md_element)
    
    def append(self, elem, elem_type='text'):
        md_text = "" #formated_element
        if elem_type in ["h1", "h2", "h3", "h4", "h5", "h6"]:
            md_text = create_header_element(elem_type, elem)
            self.append_to_content(md_text)
        elif elem_type == 'ul':
            md_text = create_unordered_list_element(elem)
            self.append_to_content(md_text)
        elif elem_type == "hr":
            self.append_to_content("---")
        elif elem_type == "tblwi": #tblwi = table with index
            md_text = crate_table_element(elem, show_index='always')+ "\n"
            self.append_to_content(md_text)
        elif elem_type == "tbl":
            md_text = crate_table_element(elem) + "\n"
            self.append_to_content(md_text)
        elif elem_type == "img":
            md_text = create_image_element(elem)
            self.append_to_content(md_text)
        else:
            self.append_to_content(elem)
