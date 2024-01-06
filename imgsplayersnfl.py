import requests
import os
import pandas as pd
from bs4 import BeautifulSoup
#from google.colab import drive

# Conectando no Google Drive
#drive.mount('/content/drive')

players = pd.DataFrame(columns=['NomeJogador','Time','Caminho'])

print(players.head())

diretorio_destino = 'C:/Users/jhow_/PycharmProjects/NFLStats/Fotos/'

jogador = ''

# Pegando o conteúdo html do site
url = 'https://www.ourlads.com/nfldepthcharts/'
site = requests.get(url)
soup = BeautifulSoup(site.content,'html.parser')

links = []
referencias = []

# Procurando os botões que contém os links de cada time (Roster é o nome que está no botão)
categorias = soup.find_all('a',string='Roster')

# Percorrendo cada botão da lista e obtendo o link dele
for link in categorias:
    links.append(link.get('href'))

# Pegando a URL padrão do site e concatenando com o link extraído do botão e colocando em uma lista para percorrer depois
for i in links:
    url = 'https://www.ourlads.com/nfldepthcharts/' + i
    referencias.append(url)

print(referencias)

# Procurando diretório e se caso não existir, cria-lo
if not os.path.exists(diretorio_destino):
    os.makedirs(diretorio_destino)

# Percorrendo cada link dentro da lista referencias
for ref in referencias:

  url = ref
  site = requests.get(url)
  soup = BeautifulSoup(site.content,'html.parser')

  # Obtendo o nome do time e tratando esse nome retirando o texto ' Roster' e trocando espaços por underline
  nome_time = soup.find('h1').text.strip().replace(' Roster','')
  time = nome_time.replace(' ','_')
  table = soup.find('table')

  link_players = []

  # Percorrendo as linhas da tabela e coletando os links que tem as páginas individuais dos jogadores
  for row in table.tbody.find_all('a',href=True):
    linhas = row.get('href')
    link_players.append(linhas)

  # Percorrendo a lista que contém os links de imagem dos jogadores
  for url_imgs in link_players:

    url = url_imgs
    site = requests.get(url)
    soup = BeautifulSoup(site.content,'html.parser')

    # Trazendo a imagem do jogador (filtrando pelo id da imagem)
    img = soup.find_all('img',id='ctl00_phContent_iHS')
    nome_player = soup.find('h1').text.strip().replace(' | ','_').replace(' ','_').replace('|','') + '_' + time  + '.png' # Obtendo nome do jogador e tratando para virar nome do arquivo .png
    caminho_destino = os.path.join(diretorio_destino, nome_player) # Juntando o diretório e o nome do jogador para criar o nome do arquivo e onde ele será salvo

    nomejogadorimg = soup.find('h1').text.strip().replace(' | ','_')
    print(nome_player)

    for i in img:
      jogador = i.get('src')

    complemento = jogador.replace('../../../images/players/','')

    # Tratando o link para ser dinâmico de acordo com o jogador
    LNK= 'https://www.ourlads.com/images/players/'+ complemento
    players = pd.concat([players,pd.DataFrame.from_records([{'NomeJogador':nomejogadorimg,'Time':nome_time,'Caminho':LNK}])], ignore_index=True)
  print(players.head())
print('Final da execução')
players.to_csv('ImagensJogadores.csv')

    # Salvando o arquivo no local de destino
    #with open(caminho_destino, "wb") as f:
      #f.write(requests.get(LNK).content)
