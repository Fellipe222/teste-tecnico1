import sqlite3, re

class Database:
    def __init__(self):
        self.conn = sqlite3.connect('db_teste')
        self.c = self.conn.cursor()

    def create_table(self,table_name:str, columns:dict):
        '''Cria uma tabela, seguindo os parâmetros fornecidos.
        
        O parâmetro "columns" deve seguir o padrão: {"nome_da_coluna" : "tipo_de_dado"}
        e.g.: {"primeiro_nome":"text"}
        
        Os tipos de dados utilizáveis no sqlite são (veja mais em: https://www.sqlite.org/datatype3.html):
        
        - NULL. The value is a NULL value.
        - INTEGER. The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
        - REAL. The value is a floating point value, stored as an 8-byte IEEE floating point number.
        - TEXT. The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
        - BLOB. The value is a blob of data, stored exactly as it was input.
        '''
        list_columns = [f'{k} {v}' for k,v in columns.items()]

        query_columns = ''
        for item in list_columns:
            query_columns += str(item)
            if list_columns.index(item) < (len(list_columns) - 1):
                query_columns += ', '                
        
        query = f'CREATE TABLE {table_name} ({query_columns});'
        print(query)
               
        with self.conn:
            try:
                self.c.execute(query)
                print(f'Tabela: {table_name} criada!')
            except sqlite3.OperationalError:
                print(f"A tabela {table_name} já existe!")
        
    def insert_data(self,table_name:str,data:list):
        '''Insere os dados fornecidos na tabela especificada.

        Exemplo de estrutura de "data": [{"nome":"joao","idade":21},
                                         {"nome":"zeca","idade":42}]
        '''
        for linha in data:
            
            list_keys = list(linha.keys())
            list_values = list(linha.values())
            tuple_values = tuple()
            keys, values = '', ''
            
            for item in list_values:
                padrao_data = re.compile(r'[0-9]{1,2}[/][0-9]{1,2}[/]{1}[0-9]{4}')
                busca = padrao_data.search(str(item))
                data,mes,dia,ano = '','','',''
                
                if busca:
                    data = str(busca.group())
                    data = data.split(sep="/")

                    mes = '0'+data[0] if len(data[0])==1 else data[0]
                    dia = '0'+data[1] if len(data[1])==1 else data[1]
                    ano = data[2]
                    
                    data = f'{ano}-{mes}-{dia}'
                                         
                tuple_values += (data,) if data!='' else (item,)    

            for item in list_keys:
                keys += str(item)
                if list_keys.index(item) < (len(list_keys) - 1):
                    keys += ', '  

            query = f'INSERT INTO {table_name} ({keys}) VALUES {tuple_values};'
            print(query)
            with self.conn:
                self.c.execute(query)
                print(f'{str(tuple(linha.values()))} salvos na tabela {table_name}')

    def show_tables(self):
        self.c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        print(self.c.fetchall())

columns_produto = {
    'CD_PROD':'INTEGER', 
    'DESC_PROD':'TEXT'
    }
columns_cliente = {
    'CPF_CNPJ':'TEXT', 
    'NOME':'TEXT', 
    'TP_PESSOA':'TEXT'
    }
columns_contrato = {
    'CD_CONTRATO':'INTEGER', 
    'CPF_CNPJ':'INTEGER', 
    'CD_PROD':'INTEGER', 
    'DT_AQUISICAO':'DATE'
    }

table_produto = [
    {'CD_PROD':1, 'DESC_PROD':'FINANCIAMENTO'},
    {'CD_PROD':2, 'DESC_PROD':'CAPITALIZAÇÃO'},
    {'CD_PROD':3, 'DESC_PROD':'CONSORCIO'},
    {'CD_PROD':4, 'DESC_PROD':'CARTAO CREDITO'}
    ]
table_cliente = [
    {'CPF_CNPJ':'36581900864'    , 'NOME':'ZÉ'      , 'TP_PESSOA':'PF'},
    {'CPF_CNPJ':'45672364809'    , 'NOME':'MARIA'   , 'TP_PESSOA':'PF'},
    {'CPF_CNPJ':'78892234895'    , 'NOME':'JOAO'    , 'TP_PESSOA':'PF'},
    {'CPF_CNPJ':'21187690866'    , 'NOME':'MURILO'  , 'TP_PESSOA':'PF'},
    {'CPF_CNPJ':'57898734000101' , 'NOME':'MANOEL'  , 'TP_PESSOA':'PJ'}
    ]
# De acordo com a ISO8601, recomenda-se que as datas sejam armazenadas no formato YYYY-MM-DD
table_contrato = [
    {'CD_CONTRATO':4341, 'CPF_CNPJ':21187690866     , 'CD_PROD': 2, 'DT_AQUISICAO': '4/3/2000'}, # MM-DD-YYY
    {'CD_CONTRATO':5431, 'CPF_CNPJ':57898734000101  , 'CD_PROD': 4, 'DT_AQUISICAO': '2/13/2001'},
    {'CD_CONTRATO':665 , 'CPF_CNPJ':45672364809     , 'CD_PROD': 1, 'DT_AQUISICAO':	'3/2/2001'},
    {'CD_CONTRATO':874 , 'CPF_CNPJ':57898734000101  , 'CD_PROD': 3, 'DT_AQUISICAO':	'4/17/2003'},
    {'CD_CONTRATO':542 , 'CPF_CNPJ':36581900864	    , 'CD_PROD': 3, 'DT_AQUISICAO': '9/8/2004'}
    ]


# Instanciando a classe:
db = Database()

# Criando as tabelas:
db.create_table(table_name="PRODUTO",columns=columns_produto)
db.create_table(table_name="CLIENTE",columns=columns_cliente)
db.create_table(table_name="CONTRATO",columns=columns_contrato)

# Populando as tabelas:
db.insert_data(table_name="PRODUTO", data=table_produto)
db.insert_data(table_name="CLIENTE", data=table_cliente)
db.insert_data(table_name="CONTRATO", data=table_contrato)


db = Database()
db.c.execute("""
SELECT B.CPF_CNPJ AS 'CPF', B.NOME, B.TP_PESSOA AS 'TIPO PESSOA', C.CD_CONTRATO AS 'CODIGO DE CONTRATO', C.DT_AQUISICAO AS 'DATA DE AQUISICAO', A.DESC_PROD AS 'DESCRICAO PRODUTO'
FROM (
    (CLIENTE B INNER JOIN CONTRATO C ON C.CPF_CNPJ=B.CPF_CNPJ)
    INNER JOIN PRODUTO A ON C.CD_PROD=A.CD_PROD
    )
WHERE B.TP_PESSOA = 'PF'
AND C.DT_AQUISICAO > '2000-04-03'
order by B.NOME
""")
[print(linha) for linha in db.c.fetchall()]

