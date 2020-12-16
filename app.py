import psycopg2
import mysql.connector
import time

from config import config

resultado = []

def connectPostgre(sql, tipo):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
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
                time.sleep(60)
                main()
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

def verificandoImagens():

    print('Verificando se possui imagens para ser verificadas!')

    resultadosIMG = connectPostgre("SELECT patient.pat_id, study.accession_no, study.pk FROM study INNER JOIN patient ON patient.pk = study.patient_fk WHERE study.enviado = 0", 1)

    if resultadosIMG[1] == None:
        print('Accession Number nulo, será necessário atualiza-lo manualmente!\nAtualizando informações para ignorar esta linha.\n')
        atualizarImagem = connectPostgre("UPDATE study SET enviado=1 WHERE pk = '{}'".format(resultadosIMG[2]), 2)

        if(atualizarImagem[0] == 'Dados Atualizados!'):
            print('Informações atualizadas!')
        else:
            print('Erro ao atualizar informações!')

    elif resultadosIMG != None:
        patientID = resultadosIMG[0]
        accession_number = resultadosIMG[1]

        print('\n----------- Exame para ser verificado encontrado! -----------')
        print('Codigo do Paciente:', patientID)
        print('Accession Number:', accession_number)
        print('-------------------------------------------------------------')
        print('\nVerificando se o exame possuí imagem no site...\n')

        resultadosSite = connectMySQL("SELECT * FROM sytb_resultados_imagens WHERE ascession_number = '{}'".format(accession_number), 1)

        if resultadosSite != None:
            print('Resultado encontrado no site =)\nAtualizando informações!\n')
            atualizarSite = connectMySQL("UPDATE sytb_resultados_imagens SET cod_animal = '{}', possui_imagem = 1 WHERE ascession_number = '{}'".format(patientID, accession_number), 2)
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
            inserirSite = connectMySQL('INSERT INTO sytb_resultados_imagens(ascession_number, cod_animal, possui_imagem) VALUES ("{}","{}",1)'.format(accession_number,patientID), 2)
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

    ''' Método  1
    
    print('Verificando se possui imagens para ser verificadas!')

    resultadosSite = connectMySQL("SELECT r.ascession_number, r.cod_animal, r.os FROM sytb_resultados_imagens AS r INNER JOIN sytb_exames AS e ON e.cod_exame = r.cod_exame INNER JOIN sytb_exame_cat AS c ON c.id = e.cat_exame WHERE c.parent = 18 AND possui_imagem = 0", 1)

    if resultadosSite != None:
        accession_number = resultadosSite[0]
        patientID = resultadosSite[1]
        os = resultadosSite[2]

        print('\n----------- Exame para ser verificado encontrado! -----------')
        print('Codigo do Paciente:', patientID)
        print('Accession Number:', accession_number)
        print('OS:', os)
        print('--------------------------------------------------------------')
        print('\nVerificando se o exame possuí imagem no servidor de imagens...\n')

        resultadosIMG = connectPostgre("SELECT patient.pat_id, study.accession_no FROM study INNER JOIN patient ON patient.pk = study.patient_fk WHERE study.enviado = 0 AND study.accession_no = '{}'".format(accession_number), 1)

        if resultadosIMG != None:
            patientID = resultadosIMG[0]
            accession_number = resultadosIMG[1]

            print('\nImagem encontrada =)\n')
            print('PatientID: {}\nAccession_Number: {}'.format(patientID, accession_number))
            print('\nVerificando as outras linhas da OS e alterando o Accession Number no banco de dados do site!\n')
            #atualizarSite = connectMySQL("UPDATE sytb_resultados_imagens AS r INNER JOIN sytb_exames e ON r.cod_exame = e.cod_exame SET r.ascession_number = {}, r.possui_imagem = 1 WHERE r.os = {} AND e.cat_exame = 18".format(accession_number, os), 2)
            atualizarSite = connectMySQL("UPDATE sytb_resultados_imagens AS r INNER JOIN sytb_exames e ON r.cod_exame = e.cod_exame INNER JOIN sytb_exame_cat c ON c.id = e.cat_exame SET r.ascession_number = {}, r.cod_animal = '{}', r.possui_imagem = 1 WHERE r.os = {} AND c.parent=18".format(accession_number, patientID, os), 2)

            if(atualizarSite[0] == 'Dados Atualizados!'):
                print(atualizarSite[0])
                print('\nAtualizando informações no banco do servidor de imagens!\n')
                atualizarImagem = connectPostgre("UPDATE study SET enviado=1 WHERE accession_no = '{}'".format(accession_number), 2)

                if(atualizarImagem[0] == 'Dados Atualizados!'):
                    print(atualizarImagem[0])
                else:
                    print('Ocorreu algum problema ao atualizar as informações no servidor de imagens.')
            else:
                print('Ocorreu algum problema ao atualizar as informações no site.')
        else:
            print('Não foi encontrado nenhuma imagem no servidor de imagens! Alterando a linha no site!')
            atualizando = connectMySQL("UPDATE sytb_resultados_imagens SET possui_imagem = 1 WHERE ascession_number='{}'".format(resultadosSite[0]), 2)

            print(atualizando[0])
    else:
        print('Nenhum resultado para ser verificado foi encontrado!')'''

def main():
    while True:
        verificandoImagens()
        print('\nAguardando 30 segundos para verificar novamente!\n')
        time.sleep(30)

if __name__ == "__main__":
    main()