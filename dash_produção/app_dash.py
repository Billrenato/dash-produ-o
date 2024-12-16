import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
import configparser
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
import dash_bootstrap_components as dbc
from dash.dash_table.Format import Group
from dash import dash_table
import plotly.graph_objects as go



config = configparser.ConfigParser()
config.read('config.ini')


username = config['database']['username']
password = config['database']['password']
host = config['database']['host']
database = config['database']['database']

##CONEXÃO DIRETO COM O BANCO UTILIZANDO SQLALCHEMY

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], assets_folder="assets")
engine = create_engine(f'firebird+fdb://{username}:{password}@{host}/{database}')

conn = engine.connect()


 ##QUERY NO BANCO DE DADOS##
sql1 = """select * from ind_cadprocessos"""

sql2 = """select ind_op.id,ind_baixa_processos.processo,produtos.descricao, sp_ind_op_status.status_op,produtos.cod,ind_op_itens.qtd,ind_baixa_processos.dh_baixa,  prod_processos.codprod ,prod_processos.codprocesso
from ind_op
join ind_op_itens on ind_op_itens.op = ind_op.id
left join ind_baixa_processos on ind_baixa_processos.op = ind_op.id
left join sp_ind_op_status(ind_op.id)  on 1=1
join produtos on produtos.cod = ind_op_itens.codpro
join prod_processos on prod_processos.codprod = produtos.cod
where (sp_ind_op_status.status_op <> 'FINALIZADA')
order by  ind_op.id,ind_baixa_processos.dh_baixa,produtos.cod, prod_processos.codprocesso"""


##tabela com op e processo

df1 = pd.read_sql_query (sql1,conn)
df2 = pd.read_sql_query (sql2,conn)


if df2['dh_baixa'].dtype != 'datetime64[ns]':
    df2['dh_baixa'] = pd.to_datetime(df2['dh_baixa'], errors='coerce')

processos = df2.groupby('cod')['codprocesso'].apply(list).to_dict()

def proximo_processo(cod, processo_atual):
    processos_cod = processos.get(cod, [])
    
    # Se processo_atual for None ou Null, retorna o primeiro processo
    if pd.isnull(processo_atual) or processo_atual == '':
        return next((x for x in processos_cod if x > 0), None)
    
    # Se processo_atual não estiver na lista, retorna processo_atual
    if processo_atual not in processos_cod:
        return processo_atual
    
    # Se processo_atual for o último processo, retorna processo_atual
    if processo_atual == processos_cod[-1]:
        return processo_atual
    
    # Retorna o próximo processo maior que 0
    indice = processos_cod.index(processo_atual)
    proximo = next((x for x in processos_cod[indice+1:] if x > 0), None)
    return proximo

df2['próximo_processo'] = df2.apply(lambda row: proximo_processo(row['cod'], row['processo']), axis=1)

resultado = df2.drop_duplicates(subset=['id', 'cod'], keep='last')[['id', 'cod','descricao' ,'processo','status_op', 'próximo_processo', 'qtd']].reset_index(drop=True)
df_quantidade_por_processo = resultado.groupby('próximo_processo')['qtd'].sum().reset_index()
df_processos = df_quantidade_por_processo.rename(columns={'próximo_processo': 'processo'})
df = df_processos.rename(columns={ 'processo': 'id'})
df_final = pd.merge( df, df1, on='id')
df_final = df_final.rename(columns={'descricao':'processo', })
df_final = df_final[['processo','qtd']]
#---------------------------------------------------------------------------------
resultado2 = resultado[['id', 'cod','descricao', 'qtd','status_op', 'próximo_processo',]]
df_todos =  resultado2.rename(columns={'id':'op', 'próximo_processo': 'id'})
df_todos2 = pd.merge( df_todos, df1, on='id')
df_todos2 =  df_todos2.rename(columns={'descricao_x':'produto','descricao_y':'processo','status_op':'status','qtd':'quantidade'})
df_todos2 = df_todos2[['op', 'cod','produto', 'quantidade','status', 'processo']]
##--------------------------------------------------------------------------------------
df_status = df_todos2.groupby('status')['quantidade'].sum().reset_index()


@app.callback(
    [Output("grafico", "figure"), Output("tabela-dados-selecionados", "data")],
    [Input("botao-voltar", "n_clicks"), Input("grafico", "clickData")]
)
def atualizar_grafico_e_tabela(n_clicks, clickData):
    ctx = dash.callback_context
    prop = ctx.triggered[0]["prop_id"].split(".")[0]
    
    if prop == "botao-voltar" or not ctx.triggered:
        fig = px.bar(df_final, y="processo", x="qtd", barmode="group", text_auto=True)
        fig.update_traces(textfont_size=20, marker_color="darkslateblue",textangle=0)
        fig.update_layout(title='Quantidade por Processo',title_x=0.5,xaxis_title='Quantidade',yaxis_title='Processo')
        
        dados_tabela = pd.merge(df_final, df_todos2, on="processo").to_dict("records")
    else:
        ponto_clicado = clickData['points'][0]
        coluna_clicada = ponto_clicado['y']
        fig = px.bar(df_final, y="processo", x="qtd", barmode="group", text_auto=True)
        fig.update_traces(textfont_size=20,textangle=0, marker_color=["red" if processo == coluna_clicada else "darkslateblue" for processo in df_final["processo"]])
        fig.update_layout(title='Quantidade por Processo',title_x=0.5,xaxis_title='Quantidade',yaxis_title='Processo')
        
        df_filtrado = df_final[df_final["processo"] == coluna_clicada]
        dados_tabela = pd.merge(df_filtrado, df_todos2, on="processo").to_dict("records")
    
    return fig, dados_tabela



fig2 = px.bar(df_status, x='status',y='quantidade',text_auto=True)
fig2 = fig2.update_traces(textfont_size=20,marker_color='#EF553B')
fig2.update_layout(title='Status',title_x=0.5,xaxis_title='',yaxis_title='Quantidade',xaxis=dict(tickangle=45))



card_grafico2 = dbc.Card([dcc.Graph(id="grafico2", figure=fig2)], inverse=True)



card_grafico = [
    dcc.Graph(id="grafico"),
    
]



app.layout = html.Div([
    # Cabeçalho
    html.H1(
        "RESUMO GERAL DE ORDEM DE PRODUÇÃO",
        style={
            "text-align": "center",
            "color": "white",
            "background-color": "slategray",
            "padding": "10px",
            "border-radius": "10px"
        }
    ),
    html.Div([
        dbc.Button("RECARREGAR", 
           id="botao-voltar",
           className="pulse", 
           color="light", 
           style={"background-color": "#FFFFFF", 
                  "color": "#2F4F4F", 
                  "border": "none", 
                  "width": "99%", 
                  "border-radius": "10px", 
                  "box-shadow": "0 0 20px rgba(0, 0, 0, 0.4)",
                  "margin": "auto",
                  "margin-top": "5px",
                  "display": "block"
                })
    ]),
    
    dbc.Row(
        [
            dbc.Col(
                dbc.Col(card_grafico2,),sm=3,md=3, lg=3, style={"padding-right": "0px",}

            ),
            dbc.Col(
                dbc.Card(
                    card_grafico, inverse=True ),sm=9, md=9, lg=9,style={"padding-left": "5px"}
            )
        ],style={"gap": "0px"}
    ),
    
    # Tabela de dados selecionados
    html.Div(
        [
            dash_table.DataTable(
                id="tabela-dados-selecionados",
                columns=[
                    {"name": "OP", "id": "op"},
                    {"name": "Código Produto", "id": "cod"},
                    {"name": "Descrição", "id": "produto"},
                    {"name": "Quantidade", "id": "quantidade","type": "numeric", "format":{"specifier": ".2f"}},
                    {"name": "Status", "id": "status"}
                    
                ],
                data=df.to_dict("records"),
                style_header={
                    "text-align": "center",
                    "background-color": "slategray",
                    "color": "white",
                    "font-size": "20px"
                },
                style_data={
                    "text-align": "center",
                    "background-color": "white",
                    "border": "1px solid slategray",
                    "font-size": "16px"
                    
                },
                style_table={
                    "overflowY": "scroll",
                    "height": "550px"
                }
            )
        ],
        className="card",
        style={"margin-top": "5px", "overflow-x": "auto"}
        ) 
],
className="box_container"
)


if __name__ == "__main__":
       
      app.run_server(debug=True)
      ##app.run_server(port=8080, host='0.0.0.0')