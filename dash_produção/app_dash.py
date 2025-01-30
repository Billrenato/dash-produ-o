import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
import configparser
import os
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
import dash_bootstrap_components as dbc
from dash.dash_table.Format import Group
from dash import dash_table
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
import schedule
import threading
import time
from datetime import date
from PIL import Image
from io import BytesIO
import fdb
import numpy as np
import base64



config_path = r'C:\dash_produção\config.ini'

config = configparser.ConfigParser()
config.read(config_path)

username = config['database']['username']
password = config['database']['password']
host = config['database']['host']
database = config['database']['database']

##CONEXÃO DIRETO COM O BANCO UTILIZANDO SQLALCHEMY

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], assets_folder="assets")
engine = create_engine(f'firebird+fdb://{username}:{password}@{host}/{database}')



dsn = config['database2']['dsn']
user = config['database2']['user']
password = config['database2']['password']
conn = fdb.connect(dsn=dsn, user=user, password=password)
cur = conn.cursor()

# Executar uma consulta SQL para selecionar os dados da tabela
cur.execute( """select a.id,a.id_item_pedido ,a.foto from arq_fotos_pedido a""")
colunas = [desc[0] for desc in cur.description]

# Ler os dados da tabela
dados = cur.fetchall()

# Converter os dados lidos em um DataFrame do pandas




conn = engine.connect()
sql = """select sp_ind_produtos_em_processo.processo, sum(sp_ind_produtos_em_processo.qtd_setor)as qtd from sp_ind_produtos_em_processo group by 1"""
sql2 = """select * from SP_IND_PRODUTOS_EM_PROCESSO"""
sql4 = """select * from itenspedidovendas"""



def carregar_dados():
    global df, df2,df3,df4
    df = pd.read_sql_query(sql, engine)
    df2 = pd.read_sql_query(sql2, engine)
    df3 = pd.DataFrame(dados, columns=colunas) 
    df4 = pd.read_sql_query(sql4, engine)

def processar_dados():
    global df_filtrado_final, df2_final, df3_final, df4_filtrado_final,df2_modal_final
    df_filtrado = df[(df[['qtd']] != 0).any(axis=1)].copy()
    df_filtrado_final = df_filtrado.sort_values(by='qtd', ascending=True)
    df2_filtrado = df2[(df2[['qtd_setor']] != 0).any(axis=1)].copy()
    df2_filtrado2 = df2_filtrado[['op','nm','status_op','codprod','descricao','processo','produzido','qtd_setor','prazo','nome']]
    df2_filtrado_final = df2_filtrado2.sort_values(by='prazo', ascending=True)
   
    df3.columns = ['id', 'id_foto', 'foto']
    df3_final = df3
    
    df4_filtrado = df4[['nm','codpro','id']].copy()
    df4_filtrado_final = df4_filtrado.rename(columns={'id': 'id_foto','codpro':'codprod'})
    df2_filtrado_final2 = pd.merge(df4_filtrado_final, df2_filtrado_final, on=['codprod','nm'], how='inner')
    df2_final = df2_filtrado_final2[['op','status_op','codprod','descricao','processo','produzido','qtd_setor','prazo','nome','id_foto']]
    df2_modal = df2_final.rename(columns={'codprod':'codigo','qtd_setor':'qtd_total'})
    df2_modal_final = df2_modal[['op','codigo','descricao','processo','produzido','qtd_total']]
    
    
    
def atualizar_df():
    carregar_dados()
    processar_dados()

def agendamento():
    schedule.every(5).seconds.do(atualizar_df) # Atualiza a cada 5 segundos
    while True:
        schedule.run_pending()
        time.sleep(1)

thread = threading.Thread(target=agendamento)
thread.daemon = True
thread.start()

def converter_blob_para_jpeg(df3_final ):
    for index, row in df3_final .iterrows():
        if isinstance(row['foto'], fdb.fbcore.BlobReader):
            blob = row['foto'].read()
            imagem = Image.open(BytesIO(blob))
            buffer = BytesIO()
            imagem.save(buffer, format="JPEG")
            df3_final .loc[index, 'foto'] = buffer.getvalue()
    return df3_final 



      
        
data_atual = date.today().strftime("%Y-%m-%d")
condicao_prazo = [
    {
        'if': {'filter_query': f'{{prazo}} < "{data_atual}"'},
        'column_id': 'op',
        ##'backgroundColor': 'rgba(255, 0, 0, 0.2)',
        'color': 'red'
    },
    {
        'if': {'filter_query': f'{{prazo}} < "{data_atual}"'},
        'column_id': 'prazo',
        ##'backgroundColor': 'rgba(255, 0, 0, 0.2)',
        'color': 'red'
    }
]


accordion = dbc.Accordion(
    [
        dbc.AccordionItem(
            [
                dbc.Card(
                    dcc.Graph(id="grafico", style={"height": "300px"}),
                    inverse=True
                )
            ],
            title=html.Div("           GRÁFICO")
        )
    ],
    start_collapsed=False,  # Começa aberto
    id="accordion",
    style={"color": "slategray", "border-radius": "10px"}
)


app.layout = html.Div([
    dcc.Interval(id="interval", interval=120000),
    # Cabeçalho
    html.H1("RESUMO GERAL DE ORDEM DE PRODUÇÃO", style={"text-align": "center", "color": "white", "background-color": "slategray", "padding": "10px", "border-radius": "10px"}),
    
    # Botão recarregar
    dbc.Button("RECARREGAR", id="botao-voltar", style={"background-color": "#FFFFFF", "color": "#2F4F4F", "border": "none", "width": "100%", "border-radius": "10px", "box-shadow": "0 0 20px rgba(0, 0, 0, 0.4)", "margin": "auto", "margin-top": "5px", "display": "block"}),
    
    # Gráfico
    dbc.Row(
        dbc.Col(accordion, sm=12, md=12, lg=12)
        ),
    html.Div(id="coluna_selecionada"),

    
    # Tabela de dados selecionados
    dbc.Col(
    html.Div(
        dash_table.DataTable(
            id="tabela-dados-selecionados",
            columns=[
                {"name": "OP", "id": "op"},
                {"name": "prazo", "id": "prazo", 'type': 'datetime'},
                {"name": "Código", "id": "codprod"},
                {"name": "Produto", "id": "descricao"},
                {"name": "Status", "id": "status_op"},
                {"name": "Vendedor", "id": "nome"}, 
                {"name": "Quantidade", "id": "qtd_setor"}
                
                
                
            ],
            style_header={
                "text-align": "center",
                "background-color": "slategray",
                "color": "white",
                "font-size": "14px"
            },
            style_data={
                "text-align": "center",
                "background-color": "white",
                "color": "slategray",
                "border": "1px solid slategray",
                "font-size": "14px"
            },
            style_table={
                "overflowY": "scroll",
                "height": "550px"
            },
            style_data_conditional=condicao_prazo
        ),
      className="table-responsive" ,style={"margin-top": "5px","border-radius": "10px", "overflow-x": "auto","box-shadow": "0 0 20px rgba(0, 0, 0, 0.4)"}
    ),sm=12, md=12, lg=12
  ),
    dbc.Modal(
        id="modal-imagem",
        children=[
            dbc.ModalHeader("Produto Selecionado"),
            dbc.ModalBody(html.Div(id="imagem-selecionada-modal", style={"box-shadow": "0 0 20px rgba(0, 0, 0, 0.4)", "width": "100%","border-radius": "10px", "height": "100%", "object-fit": "contain"})),
            
            
            ]
        ),
    dbc.Modal(
        id="modal-op",
       children=[
            dbc.ModalHeader("Informações da OP"),
            dbc.ModalBody(html.Div(id="informacoes-op",style={"box-shadow": "0 0 20px rgba(0, 0, 0, 0.4)", "width": "650px","border-radius": "10px", "height": "100%", "object-fit": "contain"}))
           ]
       )
 ], style={"margin": "30px"}
)

@app.callback(
    [Output("grafico", "figure"),
     Output("tabela-dados-selecionados", "data")],
    [Input("interval", "n_intervals"),
     Input("botao-voltar", "n_clicks"),
     Input("grafico", "clickData")],
    [dash.dependencies.State("tabela-dados-selecionados", "data")]
)
def atualizar_grafico_e_tabela(n_intervals, n_clicks, clickData, data_tabela):
    ctx = dash.callback_context
    prop = ctx.triggered[0]["prop_id"].split(".")[0]
    if prop == "interval":
        return atualizar_grafico_completo()
    elif prop == "botao-voltar" or not ctx.triggered:
        return atualizar_grafico_completo()
    elif clickData:
        return atualizar_grafico_filtrado(clickData)
    else:
        return atualizar_grafico_completo()

def atualizar_grafico_completo():
    fig = px.bar(df_filtrado_final, y="processo", x="qtd", barmode="group", text_auto=True)
    fig.update_traces(textfont_size=18, marker_color="darkslateblue", textangle=0)
    fig.update_layout(
        title='Quantidade por Processo',
        title_x=0.5,
        title_font_size=24,
        xaxis_title='Quantidade',
        xaxis_title_font_size=18,
        yaxis_title='Processo',
        yaxis_title_font_size=18
    )
    dados_tabela = pd.merge(df_filtrado_final, df2_final, on="processo").sort_values(by=['prazo', 'op'], ascending=[True, True]).to_dict("records")
    return fig, dados_tabela

def atualizar_grafico_filtrado(clickData):
    ponto_clicado = clickData['points'][0]
    coluna_clicada = ponto_clicado['y']
    fig = px.bar(df_filtrado_final, y="processo", x="qtd", barmode="group", text_auto=True)
    fig.update_traces(
        textfont_size=20,
        textangle=0,
        marker_color=["orangered" if processo == coluna_clicada else "darkslateblue" for processo in df_filtrado_final["processo"]]
    )
    fig.update_layout(
        title='Quantidade por Processo',
        title_x=0.5,
        xaxis_title='Quantidade',
        yaxis_title='Processo'
    )
    df_filtrado = df[df["processo"] == coluna_clicada]
    dados_tabela = pd.merge(df_filtrado, df2_final, on="processo").sort_values(by=['prazo', 'op'], ascending=[True, True]).to_dict("records")
    return fig, dados_tabela


@app.callback(
    Output("modal-imagem", "is_open"),
    [Input("interval", "n_intervals"), Input("tabela-dados-selecionados", "active_cell")],
    [dash.dependencies.State("modal-imagem", "is_open")]
)
def toggle_modal_imagem(n_intervals, active_cell, is_open):
    ctx = dash.callback_context
    prop = ctx.triggered[0]["prop_id"].split(".")[0]
    if prop == "interval":
        return False
    elif active_cell and 'column_id' in active_cell:
        if active_cell['column_id'] == 'descricao':
            return not is_open
    return is_open


imagem_cache = {}

@app.callback(
    Output("imagem-selecionada-modal", "children"),
    [Input("tabela-dados-selecionados", "active_cell")],
    [dash.dependencies.State("tabela-dados-selecionados", "data")],
)
def atualizar_imagem(active_cell, data_tabela):
    if active_cell and 'column_id' in active_cell:
        if active_cell['column_id'] == 'descricao':
            # Verificar se a coluna ativa é a coluna "Produto"
            if 'row' in active_cell:
                row_index = active_cell['row']
                if row_index < len(data_tabela):
                    id_foto = data_tabela[row_index].get('id_foto')
                    imagem = df3_final.loc[df3_final['id_foto'] == id_foto, 'foto']
                    if imagem.empty:
                        return html.P("Imagem não encontrada")
                    elif imagem.values[0] is None or imagem.values[0] == '':
                        return html.P("Imagem não encontrada")
                    else:
                        imagem_bytes = imagem.values[0]
                        if isinstance(imagem_bytes, fdb.fbcore.BlobReader):
                            # Verifique se a imagem já está no cache
                            if id_foto in imagem_cache:
                                imagem_base64 = imagem_cache[id_foto]
                            else:
                                imagem_bytes = imagem_bytes.read()
                                imagem_base64 = base64.b64encode(imagem_bytes).decode("utf-8")
                                imagem_cache[id_foto] = imagem_base64
                        else:
                            # Se imagem_bytes não for um BlobReader, é porque é uma imagem JPEG
                            imagem_base64 = base64.b64encode(imagem_bytes).decode("utf-8")
                        try:
                            return html.Img(src=f"data:image/jpeg;base64,{imagem_base64}", style={"max-width": "750px", "max-height": "550px", "object-fit": "contain", "width": "100%", "height": "100%", "border-radius": "10px"})
                        except Exception as e:
                            return html.P(f"Erro ao carregar imagem: {e}")
                        
@app.callback(
    Output("imagem-selecionada-modal", "is_open"),
    [Input("botao-fechar-modal", "n_clicks")],
    [dash.dependencies.State("imagem-selecionada-modal", "is_open")],
)
def fechar_modal(n_clicks, is_open):
    if n_clicks:
        return False
    return is_open


        
           
@app.callback(
    Output("informacoes-op", "children"),
    [Input("tabela-dados-selecionados", "active_cell")],
    [dash.dependencies.State("tabela-dados-selecionados", "data")],
)
def atualizar_informacoes_op(active_cell, data_tabela):
    if active_cell and 'column_id' in active_cell:
        if active_cell['column_id'] == 'op':
            if 'row' in active_cell:
                row_index = active_cell['row']
                if row_index < len(data_tabela):
                    op = data_tabela[row_index].get('op')
                    produtos = df2_modal_final[df2_modal_final['op'] == op]
                    return html.Div(
                        [
                            html.P(
                                f"OP: {op}", 
                                className="text-bold", 
                                style={"font-size": "19px","width": "auto","background-color": "slategray", "color": "white","border-radius": "10px",}
                            ),
                            html.Hr(style={"font-size": "15px"}),
                            html.H3(
                                
                               
                            ),
                            html.Table(
                                [
                                    html.Tr(
                                        [
                                            html.Th(
                                                col, 
                                                style={"font-size": "13px"}, 
                                                className="text-bold"
                                                
                                            ) 
                                            for col in produtos.columns
                                        ]
                                    ),
                                    *[
                                        html.Tr(
                                            [
                                                html.Td(
                                                    cell, 
                                                    style={"font-size": "13px","width": "650px"}, 
                                                    className="text-muted"
                                                    
                                                ) 
                                                for cell in row
                                            ],
                                            style={"border-bottom": "1px solid #ddd","width": "100%"}
                                        ) 
                                        for row in produtos.values
                                    ]
                                ],
                                style={"width": "auto"}
                            )
                        ],
                        style={"width": "650px","border-radius": "10px", "height": "100%", "object-fit": "contain"},
                        className="container bg-light p-3"
                    )
    return None


@app.callback(
    Output("modal-op", "is_open"),
    [Input("tabela-dados-selecionados", "active_cell"), Input("interval", "n_intervals")],
    [dash.dependencies.State("modal-op", "is_open"), dash.dependencies.State("modal-imagem", "is_open")],
)
def toggle_modal_op(active_cell, n_intervals, is_open, imagem_modal_is_open):
    ctx = dash.callback_context
    prop = ctx.triggered[0]["prop_id"].split(".")[0]
    if prop == "interval":
        return False
    if imagem_modal_is_open:
        return False
    if active_cell and 'column_id' in active_cell:
        if active_cell['column_id'] == 'op':
            return not is_open
    return is_open


cur.close()
conn.close()
           
if __name__ == "__main__":
  
    ##app.run_server(debug=True)
    app.run_server(port=8080, host='0.0.0.0')