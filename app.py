import psycopg2
import mysql.connector
import time
import sys
from datetime import datetime
sys.setrecursionlimit(5000)

resultado = []
params = None
params_vertis = None
parametros_intranet = None

def connectPostgre(sql, tipo):
    """ Connect to the PostgreSQL database server """
    global params
    
    from config import config
    conn = None
    try:
        if params == None:
            # read connection parameters
            params = config.postgreConfig()

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        if(tipo == 1):
            cur.execute(sql)
            resultado = cur.fetchone()
            
            if(resultado == None):
                print('Nenhuma imagem encontrada!')
                time.sleep(60*15)
                conn.close()
                verificandoImagens()
        elif(tipo == 2):
            cur.execute(sql)
            conn.commit()
            resultado = ['Dados Atualizados!']


        # display the PostgreSQL database server version
        conn.close()
        return resultado
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def connectMySQL(sql, tipo):
    from config import config
    parametros = config.mysqlConfig()

    try:
        conn = mysql.connector.connect(**parametros)

        cursor = conn.cursor(buffered=True)

        if tipo == 1:
            cursor.execute(sql)
        
            resultado = cursor.fetchone()
        elif tipo == 2:
            try:
                cursor.execute(sql)
                conn.commit()
                resultado = ['Dados Atualizados!']
            except mysql.connector.Error as err:
                print("Erro: {}".format(err))

        cursor.close()
        conn.close()

        return resultado
    except mysql.connector.Error as err:
        resultado = ['Erro','Erro: {}'.format(err)]
        return resultado

def connectIntranet(sql, tipo):
    from config import config
    parametros_intranet = config.intranetConfig()

    try:
        conn = mysql.connector.connect(**parametros_intranet)

        cursor = conn.cursor(buffered=True)

        if tipo == 1:
            cursor.execute(sql)
        
            resultado = cursor.fetchone()
        elif tipo == 2:
            try:
                cursor.execute(sql)
                conn.commit()
                resultado = ['Dados Inseridos no Intranet!']
            except mysql.connector.Error as err:
                print("Erro: {}".format(err))

        cursor.close()
        conn.close()

        return resultado
    except mysql.connector.Error as err:
        resultado = ['Erro: {}'.format(err)]
        return resultado

def connectVertis(sql):
    """ Connect to the PostgreSQL database server """
    global params_vertis
    
    from config import config
    conn_vertis = None
    try:
        if params_vertis == None:
            # read connection parameters
            params_vertis = config.vertisConfig()

        # connect to the PostgreSQL server
        conn_vertis = psycopg2.connect(**params_vertis)

        # create a cursor
        cursor_vertis = conn_vertis.cursor()

        cursor_vertis.execute(sql)
        dados = cursor_vertis.fetchone()

        # display the PostgreSQL database server version
        conn_vertis.close()
        return dados
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def verificandoImagens():

    try:
        print('Verificando se possui imagens para serem verificadas!')

        resultadosIMG = connectPostgre("SELECT patient.pat_id, study.accession_no, study.pk, study.mods_in_study, patient.pat_name FROM study INNER JOIN patient ON patient.pk = study.patient_fk WHERE study.enviado = 0", 1)
        pk = resultadosIMG[2]
        mods_in_study = resultadosIMG[3]
        pat_name = resultadosIMG[4]
        data_atual = datetime.today()

        print(isinstance(resultadosIMG[1], int))

        if resultadosIMG[1] == None:
            print('Accession Number nulo, será necessário atualiza-lo manualmente!\nAtualizando informações para ignorar esta linha.\n')
            atualizarImagem = connectPostgre("UPDATE study SET enviado=1 WHERE pk = '{}'".format(resultadosIMG[2]), 2)

            patientID = resultadosIMG[0]
            accession_number = resultadosIMG[1]

            intranet = connectIntranet('INSERT INTO imagens_failed_jobs(pk, pat_id, pat_name, mods_in_study, created_at, updated_at) VALUES("{}", "{}", "{}", "{}", "{}", "{}")'.format(pk, patientID, pat_name, mods_in_study, data_atual, data_atual), 2)

            if intranet[0] == 'Dados Inseridos no Intranet!':
                print(intranet[0])   
            else:
                print(intranet[0])
            if(atualizarImagem[0] == 'Dados Atualizados!'):
                print('Informações atualizadas!')
            else:
                print('Erro ao atualizar informações!')

        elif resultadosIMG != None:
            patientID = resultadosIMG[0]
            accession_number = resultadosIMG[1]
            print('\n----------- Exame para ser verificado encontrado! -----------')
            print('Código do Paciente:', patientID)
            print('Accession Number:', accession_number)
            print('-------------------------------------------------------------')
            print('\nProcurando informações no banco do Vertis...\n')

            info_exame = connectVertis("SELECT exame_requisitado.cod_req_exame, exame_requisitado.cod_produto, req_exame_vet.cod_clinica, req_exame_vet.cod_animal, req_exame_vet.tip_faturamento, req_exame_vet.cod_proprietario FROM exame_requisitado INNER JOIN req_exame_vet ON req_exame_vet.cod_req_exame = exame_requisitado.cod_req_exame WHERE exame_requisitado.seq_exa_requisit = '{}'".format(accession_number))

            if(info_exame != None):
                os = info_exame[0]
                cod_exame = info_exame[1]
                cod_clinica = info_exame[2]
                cod_animal = info_exame[3]
                tipo_faturamento = info_exame[4]
                cod_proprietario = info_exame[5]

                print('\n----------- Informações encontradas no BD do Vertis ----------')
                print('OS:', os)
                print('Código do Paciente:', cod_animal)
                print('Código do Exame:', cod_exame)
                print('Código da Clinica:', cod_clinica)
                print('Código do Proprietário:', cod_proprietario)
                print('Tipo do Faturamento:', tipo_faturamento)
                print('-------------------------------------------------------------')
                print('\nVerificando se o exame possuí imagem no site...\n')
            else:
                os = None
                cod_exame = None
                cod_clinica = None
                cod_animal = None
                tipo_faturamento = None
                cod_proprietario = None  

                print("\nDados não encontrados no banco de dados do Vertis\n")
                print("Cadastrando falha no intranet para correção manual...\n")

                intranet = connectIntranet('INSERT INTO imagens_failed_jobs(pk, pat_name, pat_id, mods_in_study, created_at, updated_at) VALUES("{}", "{}", "{}", "{}", "{}", "{}")'.format(pk, pat_name, patientID, mods_in_study, data_atual, data_atual), 2)
                if intranet[0] == 'Dados Inseridos no Intranet!':
                    print(intranet[0])   
                    atualizar_imagens = connectPostgre("UPDATE study SET enviado=1 WHERE accession_no = '{}'".format(accession_number), 2)
                    if(atualizar_imagens != None):
                        print("\nIgnorando exame no servidor de imagens...\n")
                    else:
                        print("\nErro ao ignorar exame no banco de imagens...\n")
                else:
                    print('Erro ao inserir informações no Intranet')

            resultadosSite = connectMySQL("SELECT * FROM sytb_resultados_imagens WHERE ascession_number = '{}'".format(accession_number), 1)

            if resultadosSite != None:
                print('Resultado encontrado no site =)\nAtualizando informações!\n')
                atualizarSite = connectMySQL("UPDATE sytb_resultados_imagens SET os = '{}', cod_exame = '{}', cod_clinica = '{}', cod_animal = '{}', tipo = '{}', cod_prop = '{}', possui_imagem = 1 WHERE ascession_number = '{}'".format(os, cod_exame, cod_clinica, patientID, tipo_faturamento, cod_proprietario, accession_number), 2)
                if(atualizarSite[0] == 'Dados Atualizados!'):
                    print('Registro atualizado na tabela do site!')
                    print('\nAtualizando tabela no servidor de imagens!\n')
                    atualizarImagem = connectPostgre("UPDATE study SET enviado=1 WHERE accession_no = '{}'".format(accession_number), 2)
                    if(atualizarImagem[0] == 'Dados Atualizados!'):
                        print('Registro atualizado com sucesso!')
                    else:
                        print('Ocorreu um erro ao atualizar as informações no servidor de imagens!')
                elif resultado[0] == 'Erro':
                    print(resultado[1])
                    verificandoImagens()
                else:
                    print('Ocorreu um erro ao atualizar as informações no site!')
            else:
                print('Imagens não cadastradas no banco do site!\nRealizando o cadastro...\n')
                inserirSite = connectMySQL('INSERT INTO sytb_resultados_imagens(os, cod_exame, cod_clinica, tipo, cod_prop, ascession_number, cod_animal, possui_imagem) VALUES ("{}", "{}", "{}", "{}", "{}", "{}","{}",1)'.format(os, cod_exame, cod_clinica, tipo_faturamento, cod_proprietario, accession_number,patientID), 2)
                if(inserirSite[0] == 'Dados Atualizados!'):
                    print('Imagens cadastradas no site! =)')
                    atualizarImagem = connectPostgre("UPDATE study SET enviado=1 WHERE accession_no = '{}'".format(accession_number), 2)
                    if(atualizarImagem[0] == 'Dados Atualizados!'):
                        print('Registro atualizado com sucesso!')
                    else:
                        print('Ocorreu um erro ao atualizar as informações no servidor de imagens!')
                elif resultado[0] == 'Erro':
                    print(resultado[1])
                    verificandoImagens()
                else:
                    print('Erro ao cadastrar as imagens no site!')
    except Exception as e:
        print(e)

        print('\nAguardando 15 minutos para tentar novamente!\n')
        time.sleep(60*15)
        main()

def timer():
    print('\nAguardando 5 minutos para verificar novamente!\n')
    time.sleep(300)
def main():
    while True:
        verificandoImagens()
        timer()
        

if __name__ == "__main__":
    main()