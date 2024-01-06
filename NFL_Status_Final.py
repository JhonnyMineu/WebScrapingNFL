import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

# Dicionário para converter os nomes coletados no site da nfl para o nome das tabelas do banco de dados
dicionario_tabelas = {'Passing': 'Passing', 'Receiving': 'Receiving', 'Rushing': 'Rushing', 'Fumbles': 'Fumbles',
                      'Tackles': 'Tackles', 'Interceptions': 'Interceptions', 'Field Goals': 'Field Goals', 'Kickoffs': 'Kickoffs',
                      'Kickoff Returns': 'Kickoff Returns', 'Punting': 'Punting', 'Punt Returns': 'Punt Returns'}


# Estabelecendo conexão com o banco de dados
engine = create_engine('mysql+mysqlconnector://root:uUpQ`a>;=[RAB%<I@35.198.48.40/NFL_Dados')

# Pegar todos os links de categorias

url = 'https://www.nfl.com/stats/'
site = requests.get(url)
soup = BeautifulSoup(site.content,'html.parser')

categorias = soup.find('ul',{'class':'d3-o-tabs d3-o-tabs__nowrap'})

# listas para armazenar os nomes dos botões e links das categorias
referencias = []
botoes = []
links = []

# Listas para armazenar o nome das tabelas e colunas do banco de dados
tabelas_banco = []
colunas_tabela = []

    # loop para pegar os textos dos botões de categorias
for i in categorias:
    ref = i
    texto = ref.text.strip()
    if i == '':
        pass
    else:
        referencias.append(texto)

    # Limpando lista anterior que vinha com sujeiras
for i in referencias:
    if i == '':
        pass
    else:
        botoes.append(i)

for i in botoes:
    categorias = soup.find(string=i)

    # obtendo os links de cada categoria dos botões para criar loop
for link in soup.find_all('a',string=['Passing', 'Rushing', 'Receiving', 'Fumbles', 'Tackles', 'Interceptions', 'Field Goals', 'Kickoffs', 'Kickoff Returns', 'Punting', 'Punt Returns']):
    # print(link.get('href'))
    links.append(link.get('href'))

# Pegar todos os links de 'próxima página' da categoria atual

for ref in range(0, len(links)):
    url = 'https://www.nfl.com' + links[ref]

    abas = [url]

    # Função para percorrer as páginas até a última
    def proximapagina(soup):
        paginas = soup.find('a', {'class': 'nfl-o-table-pagination__next'})
        if paginas is not None:
            url_base = 'https://www.nfl.com'
            prox = soup.find('a', 'nfl-o-table-pagination__next', href=True)
            url_final = (url_base + str(prox['href']))
            return url_final
        else:
            return

    # Coletando os links de próximas páginas
    while not None:
        site = requests.get(url)
        soup = BeautifulSoup(site.content,'html.parser')
        url = proximapagina(soup)
        if url is None:
            break
        abas.append(url)


    for contador in range(0,len(abas)):
        url = abas[contador]
        site = requests.get(url)
        soup = BeautifulSoup(site.content,'html.parser')
        if soup.find('table') is not None:
            pass
        else:
            del abas[contador]

    url = 'https://www.nfl.com' + links[ref]
    site = requests.get(url)
    soup = BeautifulSoup(site.content,'html.parser')


    lista = []
    nomes_co = []

    contagem_colunas = soup.find_all('th',{'class':'header'})
    colunas = soup.find('tr')

    for i in contagem_colunas:
        lista.append(i.get_text().strip())
        nomes_co.append(i.get_text().strip())

    # Obtendo o nome da página para criar um DataFrame de forma automática
    pagina = soup.find('li',{'class':'d3-o-tabs__list-item d3-is-active'})
    DF_pagina = pagina.get_text().strip()

    # Nome da tabela no banco de dados
    nome_tabela = DF_pagina

    df = pd.DataFrame(columns=nomes_co)

    # Listando as colunas do DataFrame
    data = list(df.columns)

    # Para cada categoria e links de próxima página coletados
    for numero in range(0,len(abas)):
        # Pegar todas as linhas das tabelas de cada url da lista 'próxima página'
        # Criando loop para colocar as informações do cabeçalho da tabela dentro de uma lista

        url = abas[numero]
        site = requests.get(url)
        soup = BeautifulSoup(site.content,'html.parser')
        table = soup.find('table')


        #Transformando cada linha de dado da tabela em uma linha do DataFrame de forma dinâmica
        for row in table.tbody.find_all('tr'):
            columns = row.find_all('td')

            # Obtendo colunas de cada linha
            if (columns !=[]):
                valores_coletados = {}

            # Fazendo loop dinâmico para armazenar em 'valores_coletados' cada nome da coluna e valor referente extraidos da tabela do site
                for i in range(0,(len(nomes_co))):
                    campo = columns[i].text.strip()
                    valores_coletados[data[i]] = campo

            # Condicional para só inputar no DataFrame quando a quantidade de colunas existentes estiver igual a de colunas coletadas
                    if i == (len(nomes_co)-1):
                        df = pd.concat([df,pd.DataFrame.from_records([valores_coletados])], ignore_index=True) # Input no DataFrame
                        nome = 'Nfl_dados' + str(ref)
                        df.to_csv(nome)
                        # Inserindo dados dentro do banco de dados
                        try:
                            with engine.connect() as connection:
                                df.to_sql(dicionario_tabelas[nome_tabela], con=engine, if_exists='replace', index=False)
                                connection.commit()
                        except Exception as e:
                            print(f"Erro ao inserir dados no banco de dados: {e}")
                            connection.rollback()
                        finally:
                            connection.close()
                            print(df.head(10))

# Fechando conexão como banco de dados
engine.dispose()
