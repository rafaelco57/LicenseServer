# aws_service_fastapi.py
from fastapi import FastAPI, HTTPException, Request, status, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
import psycopg2
from psycopg2 import sql, errors as psycopg2_errors
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
from apscheduler.schedulers.background import BackgroundScheduler
from collections import OrderedDict
from contextlib import contextmanager
from typing import Optional, Dict, Any
import atexit
from starlette.responses import JSONResponse

# Configuração do aplicativo FastAPI
app = FastAPI(
    title="License Manager API",
    version="1.0.0",
    description="API para gerenciamento de sessões de usuários e máquinas"
)

# Configuração CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configurações do Banco de Dados
DB_CONFIG = {
    "dbname": "LicenseManager",
    "user": "postgreeppi",
    "password": "Ppi25032025135",
    "host": "licensemanager-prd.cavov4jnjspy.sa-east-1.rds.amazonaws.com",
    "port": "5432",
    "sslmode": "require",
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3
}

# Caches
CLIENT_CACHE = OrderedDict()
MAX_CLIENT_CACHE_SIZE = 100
machine_sessions_cache = OrderedDict()
MAX_CACHE_SIZE = 100
MAX_CACHE_ITEM_SIZE = 1_000_000


# Configuração de Logging
def setup_logging():
    handler = RotatingFileHandler('aws_service.log', maxBytes=10 * 1024 * 1024, backupCount=3)
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[handler, logging.StreamHandler()]
    )


# Gerenciador de Conexão com Banco de Dados
@contextmanager
def get_db_connection():
    """Context manager para gerenciar conexões com o banco de dados"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        yield conn
    except Exception as e:
        logging.error(f"Erro na conexão com o banco: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection error"
        )
    finally:
        if conn:
            conn.close()


@contextmanager
def get_db_cursor():
    """Context manager para gerenciar cursors de banco de dados"""
    with get_db_connection() as conn:
        cursor = None
        try:
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Erro no banco de dados: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()


# Funções Auxiliares
def get_cliente_id(nome_cliente: str) -> int:
    """Obtém o ID do cliente do cache ou banco de dados"""
    if nome_cliente in CLIENT_CACHE:
        CLIENT_CACHE.move_to_end(nome_cliente)
        return CLIENT_CACHE[nome_cliente]

    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT id FROM TBLCliente WHERE nomecliente = %s", (nome_cliente,))
            result = cursor.fetchone()

            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Cliente '{nome_cliente}' não encontrado"
                )

            CLIENT_CACHE[nome_cliente] = result[0]

            # Limita o tamanho do cache
            if len(CLIENT_CACHE) > MAX_CLIENT_CACHE_SIZE:
                CLIENT_CACHE.popitem(last=False)

            return result[0]
    except Exception as e:
        logging.error(f"Erro ao buscar ID do cliente: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro interno ao buscar cliente"
        )


# Rotas da API
@app.get("/api/health", tags=["Monitoring"])
async def health_check():
    """Endpoint de verificação de saúde do serviço"""
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            if not result or result[0] != 1:
                raise Exception("Database health check failed")

        return {
            "status": "ok",
            "db_connection": True,
            "timestamp": datetime.now().isoformat(),
            "service": "license-manager-api",
            "version": "1.0.0"
        }
    except Exception as e:
        logging.error(f"Falha no health check: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service unavailable"
        )


@app.post("/api/users-sessions", tags=["Sessions"], status_code=status.HTTP_201_CREATED)
async def create_user_session(request: Request):
    """Cria uma nova sessão de usuário com verificação de existência prévia"""
    data = await request.json()
    nome_cliente = data.get('client_name') or request.headers.get('X-Client-Name')

    if not nome_cliente:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Nome do cliente não especificado"
        )

    try:
        with get_db_cursor() as cursor:
            id_cliente = get_cliente_id(nome_cliente)

            # Verifica se a sessão já existe
            cursor.execute(
                "SELECT 1 FROM user_sessions WHERE guid = %s AND id_cliente = %s",
                (data['guid'], id_cliente)
            )

            if cursor.fetchone():
                logging.warning(f"Tentativa de criar sessão duplicada: {data['guid']}")
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "session_already_exists",
                        "message": "Sessão já registrada para este cliente",
                        "guid": data['guid'],
                        "client_id": id_cliente
                    }
                )

            # Se não existir, faz o insert
            cursor.execute(
                sql.SQL("""
                    INSERT INTO user_sessions 
                    (guid, username, ip, id_user, id_cliente, dt_creation, dt_last_send)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """),
                (
                    data['guid'],
                    data['username'],
                    data['ip'],
                    data['idUser'],
                    id_cliente,
                    datetime.fromisoformat(data['dtCreation']),
                    datetime.fromisoformat(data['dtLastSend'])
                )
            )

            logging.info(f"Sessão {data['guid']} criada para o cliente {nome_cliente}")
            return {
                "status": "success",
                "message": "Session created successfully",
                "session_id": data['guid']
            }

    except HTTPException:
        raise  # Re-lança exceções HTTP que já tratamos
    except Exception as e:
        logging.error(f"Erro ao criar sessão: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.put("/api/users-sessions/{guid}", tags=["Sessions"])
async def update_user_session(guid: str, request: Request):
    """Atualiza uma sessão de usuário existente"""
    data = await request.json()
    nome_cliente = data.get('client_name') or request.headers.get('X-Client-Name')

    if not nome_cliente:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "status": "error",
                "error_type": "missing_client_name",
                "message": "Nome do cliente não especificado"
            }
        )

    try:
        with get_db_cursor() as cursor:
            id_cliente = get_cliente_id(nome_cliente)

            cursor.execute(
                "SELECT guid FROM user_sessions WHERE guid = %s AND id_cliente = %s",
                (guid, id_cliente)
            )
            if not cursor.fetchone():
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content={
                        "status": "error",
                        "error_type": "session_not_found",
                        "message": "Sessão não encontrada",
                        "details": {
                            "guid": guid,
                            "client_id": id_cliente
                        }
                    }
                )

            cursor.execute(
                sql.SQL("""
                    UPDATE user_sessions
                    SET dt_last_send = %s
                    WHERE guid = %s AND id_cliente = %s
                """),
                (
                    datetime.fromisoformat(data['dtLastSend']),
                    guid,
                    id_cliente
                )
            )

            logging.info(f"Sessão {guid} atualizada para o cliente {nome_cliente}")
            return {
                "status": "success",
                "message": "Session updated successfully",
                "updated_session": {
                    "guid": guid,
                    "last_updated": data['dtLastSend']
                }
            }

    except Exception as e:
        logging.error(f"Erro ao atualizar sessão: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "status": "error",
                "error_type": "internal_server_error",
                "message": "Erro interno no processamento da requisição"
            }
        )


@app.delete("/api/users-sessions/{guid}", tags=["Sessions"])
async def delete_user_session(guid: str, request: Request):
    """Remove uma sessão e a move para o histórico"""
    nome_cliente = request.headers.get('X-Client-Name')

    if not nome_cliente:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Header X-Client-Name é obrigatório"
        )

    try:
        with get_db_cursor() as cursor:
            id_cliente = get_cliente_id(nome_cliente)

            # Busca a sessão para mover ao histórico
            cursor.execute(
                """
                SELECT guid, username, ip, id_user, id_cliente, dt_creation, dt_last_send
                FROM user_sessions
                WHERE guid = %s AND id_cliente = %s
                """,
                (guid, id_cliente)
            )
            session = cursor.fetchone()

            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Session not found"
                )

            # Move para o histórico
            cursor.execute(
                """
                INSERT INTO user_sessions_history 
                (guid, username, ip, id_user, id_cliente, dt_creation, dt_last_send)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                session
            )

            # Remove da tabela principal
            cursor.execute(
                "DELETE FROM user_sessions WHERE guid = %s AND id_cliente = %s",
                (guid, id_cliente)
            )

            logging.info(f"Sessão {guid} movida para o histórico")
            return {"status": "success", "message": "Session deleted successfully"}

    except Exception as e:
        logging.error(f"Erro ao deletar sessão: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.post("/api/machines-sessions", tags=["Machine Sessions"])
async def handle_machine_sessions(request: Request):
    """Processa sessões de máquina (inserção ou atualização)"""
    data = await request.json()
    nome_cliente = data.get('client_name') or request.headers.get('X-Client-Name')
    sessions = data.get('sessions')

    if not nome_cliente:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Nome do cliente não especificado"
        )

    if not sessions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Campo 'sessions' é obrigatório"
        )

    try:
        with get_db_cursor() as cursor:
            id_cliente = get_cliente_id(nome_cliente)

            # Verifica se já existe no cache
            if id_cliente in machine_sessions_cache and machine_sessions_cache[id_cliente] == sessions:
                return {
                    "status": "success",
                    "message": "No changes needed"
                }

            # Verifica no banco de dados
            cursor.execute(
                """
                SELECT sessions, dtlastupdate
                FROM machine_sessions
                WHERE id_cliente = %s
                ORDER BY dtlastupdate DESC
                LIMIT 1
                """,
                (id_cliente,)
            )
            last_record = cursor.fetchone()

            if last_record and last_record[0] == sessions:
                machine_sessions_cache[id_cliente] = sessions
                return {
                    "status": "success",
                    "message": "No changes needed"
                }

            # Insere no banco
            cursor.execute(
                """
                INSERT INTO machine_sessions (id_cliente, sessions, dtlastupdate)
                VALUES (%s, %s, %s)
                """,
                (id_cliente, sessions, datetime.now())
            )

            # Atualiza cache
            machine_sessions_cache[id_cliente] = sessions
            if len(machine_sessions_cache) > MAX_CACHE_SIZE:
                machine_sessions_cache.popitem(last=False)

            logging.info(f"Sessões de máquina atualizadas para o cliente {nome_cliente}")
            return {
                "status": "success",
                "message": "Machine sessions updated"
            }

    except Exception as e:
        logging.error(f"Erro ao atualizar sessões de máquina: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


# Tarefas Agendadas
# scheduler = BackgroundScheduler({
#     'apscheduler.job_defaults.max_instances': 3,
#     'apscheduler.job_defaults.coalesce': True,
#     'apscheduler.job_defaults.misfire_grace_time': 3600
# })


def delete_inactive_sessions():
    """Remove sessões inativas migrado para banco sem uso """
    try:
        with get_db_cursor() as cursor:
            inactivity_limit = datetime.now() - timedelta(minutes=5)

            # Busca sessões inativas
            cursor.execute(
                """
                SELECT guid, username, ip, id_user, id_cliente, dt_creation, dt_last_send
                FROM user_sessions
                WHERE dt_last_send < %s
                """,
                (inactivity_limit,)
            )
            inactive_sessions = cursor.fetchall()

            if not inactive_sessions:
                return

            # Move para o histórico e deleta
            for session in inactive_sessions:
                cursor.execute(
                    """
                    INSERT INTO user_sessions_history 
                    (guid, username, ip, id_user, id_cliente, dt_creation, dt_last_send)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    session
                )
                cursor.execute(
                    "DELETE FROM user_sessions WHERE guid = %s",
                    (session[0],)
                )

            logging.info(f"Removidas {len(inactive_sessions)} sessões inativas")
    except Exception as e:
        logging.error(f"Erro ao remover sessões inativas: {str(e)}")


def count_active_sessions():
    """Conta sessões ativas - migrado para banco sem uso """
    try:
        with get_db_cursor() as cursor:
            active_limit = datetime.now() - timedelta(minutes=5)

            cursor.execute(
                """
                SELECT id_cliente, COUNT(*) 
                FROM user_sessions
                WHERE dt_last_send >= %s
                GROUP BY id_cliente
                """,
                (active_limit,)
            )
            counts = cursor.fetchall()

            for id_cliente, count in counts:
                cursor.execute(
                    """
                    INSERT INTO Session_count (id_cliente, count, dt_creation)
                    VALUES (%s, %s, %s)
                    """,
                    (id_cliente, count, datetime.now())
                )

            logging.debug(f"Contagem de sessões ativas registrada para {len(counts)} clientes")
    except Exception as e:
        logging.error(f"Erro ao contar sessões ativas: {str(e)}")


# Configuração e Inicialização
def initialize_app():
    """Inicializa o aplicativo"""
    setup_logging()

    # Agenda tarefas periódicas - migradas para task de banco de dados
    # scheduler.add_job(delete_inactive_sessions, 'interval', minutes=5)
    # scheduler.add_job(count_active_sessions, 'interval', minutes=5)
    # scheduler.add_job(lambda: CLIENT_CACHE.clear(), 'interval', hours=24)

    # scheduler.start()
    # atexit.register(lambda: scheduler.shutdown())


if __name__ == '__main__':
    import uvicorn

    initialize_app()
    uvicorn.run("src.License_Server:app", host="0.0.0.0", port=5000, reload=False)
