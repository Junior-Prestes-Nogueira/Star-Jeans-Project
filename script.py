# Imports
import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime
from collections import ChainMap
import sqlite3
import sqlalchemy

def get_url():

    #  Primeira requisição (sem saber pagesize) + Requisição na API para TODOS os produtos (com pagesize)
    url_base = 'https://www2.hm.com/en_us/men/products/jeans.html'
    page = requests.get(url_base, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:50.0) Gecko/20100101 Firefox/50.0'})
    soup = BeautifulSoup(page.text, 'html.parser')    
    max_size = int(soup.find('h2', class_='load-more-heading').get('data-total'))

    url = f'https://www2.hm.com/en_us/men/products/jeans.html?sort=stock&image-size=small&image=model&offset=0&page-size={max_size}'
    page = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:50.0) Gecko/20100101 Firefox/50.0'})
    soup = BeautifulSoup(page.text, 'html.parser')

    return soup 


def get_sku_price_name_category(soup):

    product_list = soup.find('ul', class_='products-listing small')    
    products_sku, products_category, products_name, products_price = ([], [], [], [])

    products_sku_list = product_list.find_all('article', class_='hm-product-item')
    [products_sku.append(product.get('data-articlecode')) for product in products_sku_list]
    
    products_category_list = product_list.find_all('article', class_='hm-product-item')
    [products_category.append(product.get('data-category')) for product in products_category_list]
    
    products_name_list = product_list.find_all('a', class_='link')
    [products_name.append(product.get_text(strip=True)) for product in products_name_list]

    products_price_list = product_list.find_all('span', class_='price regular')
    [products_price.append(product.get_text(strip=True)) for product in products_price_list]
   
    df_sku_price_name_category = pd.DataFrame({'SKU': products_sku, 'NAME': products_name, 'PRICE': products_price, 'CATEGORY': products_category})

    
    df_sku_price_name_category['style_id'] = df_sku_price_name_category['SKU'].apply(lambda x: x[:-3])
    df_sku_price_name_category = df_sku_price_name_category.drop_duplicates(subset="style_id", keep='first').reset_index(drop=True) # Eliminar todas as linhas que possuem valores repetidos com base na coluna -> style_id

    return df_sku_price_name_category
# Criando uma lsita com todos os sku para utilizar eles como parametro do link para acesso dos produtos:


def get_color(soup, df_sku_price_name_category):

    products_sku = list(df_sku_price_name_category['SKU'])

    # Criando dataframe para armazenar os dados com concat dentro do laço for
    df_color = pd.DataFrame()

    # Estarei iterando sobre o product_sku após a limpeza dos IDS repetidos:
    for sku in products_sku:    
        # Request and instantiating :  
        url = f'https://www2.hm.com/en_us/productpage.{sku}.html'  # Observar que o sku é o parametro para achar o html do produto especifico.    
        page = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:50.0) Gecko/20100101 Firefox/50.0'})    
        soup = BeautifulSoup(page.text, 'html.parser')

        # Criando variaveis:
        product_color, product_page_sku = ([], [])    

        # --------------- Extraindo as cores ---------------
        product_color_list = soup.find_all('a', class_='filter-option')
        [product_color.append(products.get('data-color')) for products in product_color_list]

        # --------------- Extraindo TODOS os SKU's do site  ---------------
        product_sku_list = soup.find_all('a', class_='filter-option')
        [product_page_sku.append(products.get('data-articlecode')) for products in product_color_list]


        # Criando um DataFrame para a cor do produto + o sku:
        data_page = pd.DataFrame({'SKU': product_page_sku, 'product_color': product_color})

        # Criando as variaveis style_id e color_id, para diferenciar do SKU (style_id + color_id):
        data_page['style_id'] = data_page.apply(lambda x: x['SKU'][:-3], axis='columns')
        data_page['color_id'] = data_page.apply(lambda x: x['SKU'][-3:], axis='columns')


        # Armazenando isso em um DataFrame de forma ciclica. Assim armazena de todas as iterações:
        df_color = pd.concat([df_color, data_page], ignore_index=True)

    # DataFrame completo com todas as informações sobre cor, sku e style_id de TODOS os produtos:
    return df_color


def get_size_fit_comp(soup, df_color, df_sku_price_name_category):
# Dicionario para reter as infomações coletadas dos produtos no scrapy:
    product_details_dict = {}
    product_sku_total = list(df_color['SKU'])
    
    # Acessando todos os produtos do site:
    for sku in product_sku_total:    
        # Request:
        url = f'https://www2.hm.com/en_us/productpage.{sku}.html'    
        page = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:50.0) Gecko/20100101 Firefox/50.0'})   
        soup = BeautifulSoup(page.text, 'html.parser')
        # Retirando informações mais dificeis, com tratamento de erro caso não de certo (obs: alguns links podem retornar html vazio):
        try:
            # Tag para extrair os dados:
            product_details_tag = soup.find('hm-product-description', id="js-product-description").find('dl')       
            # Irei retirar o texto de todas as tags e juntar todas essas informações dentro de uma lista. Depois irei aplicar o metodo filter() para retirar as strings vazias:                    
            info = [tags.get_text(strip=True) for tags in product_details_tag]  # Criando uma lista para armazenar as sentenças retiradas dessa tag
            # Retirando as strings vazias da lista:
            product_details = list(filter(None, info))
            # product_details.clear()   # Caso execute mais de uma vez o codigo sem querer 
            # Criando um dicionario para armazenar todos os dados relacionados de acordo com o {SKU: caracteristicas}
            product_details_dict.update({sku: product_details}) 
        except:
            print(product_sku_total.remove(sku))   # Vai ter umas  url que vao retornar vazio.
    
    # ------------  Extracting Size, Composition and FIT: ------------
    product_sku_size, product_sku_composition, product_sku_fit  = ({}, {}, {})   
    
    for sku, product in product_details_dict.items():
        
        r_comp = re.compile("Composition")
        r_size = re.compile("Size")
        r_fit = re.compile("Fit")
    
        if list(filter(r_comp.match, product)):        
            product_composition = list(filter(r_comp.match, product))        
            product_sku_composition.update({sku: product_composition[0]})
        else:
            product_sku_composition.update({sku: 'NA'}) 
    
        if list(filter(r_size.match, product)):
            product_size = list(filter(r_size.match, product))        
            product_sku_size.update({sku: product_size[0]}) 
        else:
            product_sku_size.update({sku: 'NA'}) 
    
        if list(filter(r_fit.match, product)):
            product_fit = list(filter(r_fit.match, product))
            product_sku_fit.update({sku: product_fit[0]})
        else:
            product_sku_fit.update({sku: 'NA'})

    # Creating Dataframes:
    df_size = pd.DataFrame({'Size': product_sku_size}).reset_index().rename({'index': 'SKU'}, axis='columns')
    df_fit = pd.DataFrame({'Fit': product_sku_fit}).reset_index().rename({'index': 'SKU'}, axis='columns')
    df_composition = pd.DataFrame({'Composition': product_sku_composition}).reset_index().rename({'index': 'SKU'}, axis='columns')
    
    # ----------------------------- Criar um dataframe_raw para limpeza de dados com um merge entre os datafarmes de features diferentes para um mesmo produto: -----------------------------
    # Fazeno o merge entre todas os dataframes e coletando em um df_raw:
    df_fit_size = df_size.merge(df_fit, on='SKU')
    df_fit_size_comp = df_fit_size.merge(df_composition, on='SKU')
    df_fit_size_comp_color = df_fit_size_comp.merge(df_color, on='SKU') 
    df_raw = df_fit_size_comp_color.merge(df_sku_price_name_category, on='style_id')
    
    # ----------------------------- Criando uma feature de identificação ----------------------------- 
    identification = df_raw['style_id'].drop_duplicates().to_list()   # excluindo ids repetidos
    product_identfication = [{style_id: f'product_{index}' } for index, style_id in enumerate(identification)]  # definindo a identificaçao por style_id
    product_identfication = dict(ChainMap(*product_identfication)) # Transformando uma lista de dicionarios em um unico dicionario com dicionarios
    df_raw['product_identification'] = 'NA'  # Criando a feature para preencher
    
    for index in range(len(df_raw)):
        for style_id, indentification in product_identfication.items():   
            if df_raw.loc[index, 'style_id'] == style_id:                        
                df_raw.loc[index, 'product_identification'] = indentification

    # ----------------------------- Criando feature scrapy datetime -----------------------------
    df_raw['scrapy_datetime'] = datetime.now().strftime('%Y-%M-%d %H:%S:%S')
    
    return df_raw


def cleaning_data(df_raw):

    # Deletando e renomeando features
    df_raw.drop(columns=['SKU_y', 'style_id', 'color_id'], axis='columns', inplace=True)
    df_raw.rename({'SKU_x': 'sku', 'Size': 'size', 'Fit': 'fit', 'Composition': 'composition', 'NAME': 'name', 'PRICE': 'price', 'CATEGORY': 'category'}, axis='columns', inplace=True)

    # Limpeza:
    df_raw['name'] = df_raw['name'].apply(lambda x: x.lower().replace(' ', '_') if pd.notnull(x) else x)    

    df_raw['price'] = df_raw['price'].apply(lambda x: float(x.replace('$', '')) if pd.notnull(x) else x)    

    df_raw['fit'] = df_raw['fit'].apply(lambda x: x.replace('Fit', '').lower().replace(' ', '_') if pd.notnull(x) else x)   

    df_raw['composition'] = df_raw['composition'].apply(lambda x: x.replace('Composition', '').replace('%P', '% P') if pd.notnull(x) else x)    

    df_raw['product_color'] = df_raw['product_color'].apply(lambda x: x.lower().replace(' ', '_') if pd.notnull(x) else x)

    df_raw['size'] = df_raw['size'].apply(lambda x: x.replace('Size', ''))

    df_raw['size'] = df_raw['size'].apply(lambda x: x.replace('NA','SizeThe model is 000cm/00" and wears a size 00/00')) # Alterando os valores null de size para evitar/tratar erros:

    df_raw['composition'] = df_raw['composition'].apply(lambda x: re.search('(^.+)\sPocket', x).group(1) if re.search('(^.+)\sPocket', x) else x)   # Excluindo informacoes da composicao do bolso
    df_raw['composition'] = df_raw['composition'].apply(lambda x: re.search('(^.+)\%Lining', x).group(1) if re.search('(^.+)\%Lining', x) else x)   # Excluindo informacoes da composicao do bolso
    
    # Mudando a posiçao das features
    new_cols = ["product_identification", "sku", "name", "price", "fit", "category", "composition", "product_color", "size", "scrapy_datetime"]
    df_raw = df_raw.reindex(columns=new_cols)

    return df_raw


def create_size_features(data):

    # Search for size_inches
    def size_model_inches(data):
        data['size_model_inches'] = 'NA'

        for index, rows in data['size'].iteritems():
            size_model_inches = rows
            pattern_ = '[/](.+\")' # ou -> \d{3}\w{2}\W(.+")       
            match_ = re.search(pattern_, size_model_inches ).group(0).replace('/', '')

            data.loc[index, 'size_model_inches'] = match_

        return None 

    # Search for size_model_cm
    def size_model_cm(data):
        # Extraindo os dados e manipulando a nova feature
        size_models  = ','.join(data['size'].to_list())   # Texto
        pattern_ = '\d{3}'   # Mascara
        match_ = re.findall(pattern_, size_models)  # Aplicando a busca da mascara no texto
        data['size_model_cm'] = match_
        data['size_model_cm'] = data['size_model_cm'].astype('int64')   
        data['size_model_cm'] = data['size_model_cm'].apply(lambda x: 'NA' if x < 1 else x)   
        
        return None

    # Search for size_number
    def size_number(data):
        data['size_number'] = 'NA'

        for index, rows in data['size'].iteritems():    
            size_numbers  = data.loc[index, 'size']  # texto para buscar
            mask1 = '\d{2}\/\d{2}'  # 31/32
            mask2 = '[SML]'  # extrai as letras S ou M ou L se enncontrados
            mask3 =  '\s(\d{2}$)' # extrai só apenas os numeros de 2 digitos sem barra -> ex:  31

            search1_ = re.search(mask1, size_numbers)
            search2_ = re.search(mask2, size_numbers)
            search3_ = re.search(mask3, size_numbers)

            if search1_:        
                match_ = search1_.group(0)

            elif search2_:        
                match_ = search2_.group(0)

            elif search3_:        
                match_ = search3_.group(1)

            data.loc[index, 'size_number'] = match_

        return None
    
    size_number(data)        
    size_model_cm(data)
    size_model_inches(data)
    data.drop('size', axis=1, inplace=True)

    return None


def create_comp_features(data):

    # Definindo todas as features que da pra criar com a composition:
    # Função que cria automaticamente novas features pra tipos de material de composition:

    # Aplicando regex para selecionar TODOS os atributos de composition:
    pattern_ = '[A-Za-z]+'  # Busca por todas as palavras, ou seja minhas features
    composition_text= ','.join(list(data['composition'].unique()))  # Ajustando o texto para utilizar a mascara

    composition_words = re.findall(pattern_, composition_text)    # Achando todas as palavras do texto
    composition_words = pd.Series(composition_words)    # Transformando em um series para utilizar metodos do pandas
    composition_words.drop_duplicates(inplace=True)     # Excluindo todas as palavras repetidas e mantendo a 1 aparicao 
    composition_words.reset_index(drop=True, inplace=True)
    composition_words = composition_words.to_list()     # Convertendo de volta para uma lista
    
    if 'shell' in composition_words:
        composition_words.remove('Shell')   # Removendo palavra sem utilidade, agora possuo apenas materiais de composição
    
    data_composition = pd.DataFrame(composition_words).transpose()
    data_composition.columns = data_composition.iloc[0]
    data_composition.drop(index=0, inplace=True)

    # Merge:
    data.merge(data_composition, left_index=True, right_index=True, how='outer')  # Por estar fazendo um merge c um df vazio, precisa fazer o merge pelos index e com outer para pegar tudo

    def fill_composition(data):
        for composition in data_composition.columns:
            data[composition] = data['composition'].apply(lambda x: re.search(f'{composition} \d+%', x).group(0) if re.search(f'{composition} \d+%', x) else 'NA')
            data[composition] = data[composition].apply(lambda x: float(x.replace(composition, '').replace('%', ''))/100 if x != 'NA' else 'NA')
    
        return None

    fill_composition(data)

    return None


def create_sql_data(data):
    
    # Criando um banco de dados sqlite:
    con  = sqlite3.connect('h2m_database.db')
    con.commit()
    con.close()

    # Criando conexão com o banco sqlite atraves do sqlalchemy
    con_engine = sqlalchemy.create_engine('sqlite:///h2m_database.db')

    # Metodo para criar uma tabela dentro do banco de dados h2m_database com base na tabela data criada no codigo
    data.to_sql(name='h2m_table', if_exists = 'replace', con=con_engine, index=False)    

    return None


if __name__ == '__main__':

    soup = get_url()

    df_sku_price_name_category = get_sku_price_name_category(soup)

    df_color = get_color(soup, df_sku_price_name_category)

    df_raw = get_size_fit_comp(soup, df_color, df_sku_price_name_category)
    
    data = cleaning_data(df_raw)

    create_size_features(data)
    create_comp_features(data)
    
    create_sql_data(data)     # Criando um banco de dados com a tabela