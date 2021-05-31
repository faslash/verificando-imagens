import psycopg2
import mysql.connector
import time
import sys
sys.setrecursionlimit(5000)

resultado = []
params = None

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

def verificandoImagens():

    try:
        print('Verificando se possui imagens para serem verificadas!')

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
    except Exception:
        print('Erro:'.format(Exception))
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