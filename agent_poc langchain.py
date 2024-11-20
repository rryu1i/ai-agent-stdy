
import os
from data.data_ingestion import Engine
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from operator import itemgetter

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough, RunnableLambda




DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

openai_api_key = os.getenv("OPENAI_API_KEY")

llm = ChatOpenAI(model="gpt-4o-mini", api_key=openai_api_key)

engine = Engine(**DB_CONFIG).create_engine()
db = SQLDatabase(engine)

# print(db.run("SELECT * from customers LIMIT 5"))

execute_query = QuerySQLDataBaseTool(db=db)
write_query = create_sql_query_chain(llm, db)

# chain = write_query | RunnableLambda(lambda x:x.replace('SQLQuery:','')) | execute_query

# print(chain.invoke({"question": "quantos clientes temos em sp?"}))

answer_prompt = PromptTemplate.from_template(
    """Given the following user question, corresponding SQL query, and SQL result, answer the user question.

Question: {question}
SQL Query: {query}
SQL Result: {result}
Answer: """
)

chain = (
    RunnablePassthrough.assign(query=write_query).assign(
        result=itemgetter("query") 
        | RunnableLambda(lambda x:x.replace('SQLQuery:','')) 
        | RunnableLambda(lambda x:x.replace('```','').replace('sql',''))
        | execute_query
    )
    | answer_prompt
    | llm
    | StrOutputParser()
)

print(chain.invoke({"question": "quantos customers temos no PR?"}))