# %%
import json

from src.connection import Neo4jConnection
from src.process_data import insert_data,  iter_parse_stackexchange

# %%
# NOTE: you will need to initialize a neo4j database with these credentials and start it.
port = 11003 # Check if is the case for your server!
PWD='knowledge'

conn = Neo4jConnection(uri="bolt://localhost:"+str(port),
                       user="neo4j",
                       pwd=PWD)

# %%
def drop_indexes_and_constraints():
    index = conn.query("CALL db.indexes")
    for i in index:
        print(i['name'])
        conn.query(f"DROP INDEX {i['name']}")
    const = conn.query("CALL db.constraints")
    for c in const:
        print(c['name'])
        conn.query(f"DROP CONSTRAINT {c['name']}")
# drop_indexes_and_constraints()
#%% Reset indexes and constraints

# %%
# Old indexes:
# conn.query('CREATE INDEX paper_id_index IF NOT EXISTS FOR (p:Paper) ON (p.paperid);')
# conn.query('CREATE INDEX author_id_index IF NOT EXISTS FOR (a:Author) ON (a.authorid);')
# conn.query('CREATE INDEX paper_doi_index IF NOT EXISTS FOR (p:Paper) ON (p.doi);')
# conn.query('CREATE INDEX post_id_index IF NOT EXISTS FOR (p:Post) ON (p.postid);')
# conn.query('CREATE INDEX comment_id_index IF NOT EXISTS FOR (c:Comment) ON (c.commentid);')
# conn.query('CREATE INDEX board_index IF NOT EXISTS FOR (b:Board) ON (b.boardname);')
#%%
# If necessary to reset the database:
# conn.query("CREATE LOOKUP INDEX node_label_lookup_index IF NOT EXISTS FOR (n) ON EACH labels(n);")
# conn.query("CREATE LOOKUP INDEX rel_type_lookup_index IF NOT EXISTS FOR ()-[r]-() ON EACH type(r);")
# #%%
conn.query('CREATE CONSTRAINT paper_id_key IF NOT EXISTS FOR (p:Paper) REQUIRE (p.paperid) IS UNIQUE;')
conn.query('CREATE CONSTRAINT paper_doi_key IF NOT EXISTS FOR (p:Paper) REQUIRE (p.doi) IS UNIQUE;')
conn.query('CREATE CONSTRAINT author_id_key IF NOT EXISTS FOR (n:Author) REQUIRE (n.authorid) IS UNIQUE;')
#%%
conn.query('CREATE CONSTRAINT board_key IF NOT EXISTS FOR (n:Board) REQUIRE (n.boardname) IS UNIQUE;')
conn.query('CREATE CONSTRAINT post_key IF NOT EXISTS FOR (n:Post) REQUIRE (n.board, n.postid) IS UNIQUE;')
conn.query('CREATE CONSTRAINT comment_key IF NOT EXISTS FOR (c:Comment) REQUIRE (c.board, c.commentid) IS UNIQUE;')





#%%
N_PAPERS = 4107340 + 1 # Number of papers in the dataset
PAPER_PATH = r"D:\datasets\citations\dblp_papers_v11.txt"
def iter_parse_paper_data():
    exception_count = 0
    with open(PAPER_PATH, 'r') as f:
        for line in f:
            try:
                yield json.loads(line)
            except Exception as e:
                exception_count += 1
                if exception_count > 10:
                    continue
                print(e)
                if exception_count == 10:
                    print("10 exceptions encountered. SKipping further errors.")
    print(f"While loading papers, {exception_count} exceptions encountered.")

def add_papers(batch_size=1000):
    """Adds paper nodes and relationships (:Author)-[:AUTHORED]-(:Paper), (:Paper)-[:CITES]-(:Paper)"""
    query = '''
    // Create papers
    UNWIND $rows as paper
    MERGE (p:Paper {paperid: paper.id, doi: paper.doi})
    ON CREATE SET
    p.title = paper.title,
    p.year = paper.year,
    p.n_citation = paper.n_citation

    // Match authors
    WITH paper, p
    UNWIND  paper.authors AS author
    MERGE (a:Author {authorid: author.id})
    ON CREATE SET a.name = author.name
    MERGE (a)-[:AUTHORED]->(p)
    RETURN count(p) as total
    '''
    query2= '''
    // Match references
    UNWIND $rows as paper
    MATCH (p:Paper {paperid: paper.id})
    WITH paper, p
    UNWIND  paper.references AS refid
    MATCH (r:Paper {paperid:refid})
    MERGE (p)-[cit:CITES]->(r)
    RETURN count(cit) as total
    '''
    print("Adding papers...")
    res1 = insert_data(conn, query,
                       record_iterator=iter_parse_paper_data(), 
                       batch_size=batch_size, 
                       total=N_PAPERS)
    print("Adding papers citations...")
    res2 = insert_data(conn, query2,
                       record_iterator=iter_parse_paper_data(), 
                       batch_size=batch_size, 
                       total=N_PAPERS)
    print("Done.")
    return res1, res2



# %%




# %%
def add_boards(board_list, batch_size=50):
    # Adds board nodes
    query = '''
    UNWIND $rows as board
    MERGE (b:Board {boardname: board})
    RETURN count(b:Board) as total
    '''
    return insert_data(conn, query, board_list, batch_size)

# %%
def add_posts(board, batch_size=50):
   # Adds post nodes
   query = '''
    // Create posts
    UNWIND $rows as post
    MERGE (p:Post {postid: post.Id, board: '%(board)s'})
    ON CREATE SET
    p.title = post.Title,
    p.text = post.Body,
    p.tags = post.Tags,
    p.score = post.Score

    WITH post, p
    UNWIND post.DOIs AS doi
    MATCH (r:Paper {doi:doi})
    MERGE (p)-[:CITES]->(r)

    WITH post, p
    MATCH (b:Board {boardname: '%(board)s'})
    MERGE (p)-[fb:FROM_BOARD]->(b)
    RETURN count(fb) as total
   ''' % dict(board=board)
   post_record_generator = iter_parse_stackexchange(board, 'Posts')
   return insert_data(conn, query, post_record_generator, batch_size)


def test_board_links(board, batch_size=50):
   # Adds post nodes
   query = '''
    UNWIND $rows as post
    OPTIONAL MATCH (p:Paper {postid: post.Id, board: '%(board)s'})
    MATCH (b:Board {boardname:  '%(board)s'})
    RETURN count(b), count(p)
   ''' % dict(board=board)
   print(query)
   post_record_generator = iter_parse_stackexchange(board, 'Posts', max_num=5)
   return insert_data(conn, query, post_record_generator, batch_size)
# test_board_links('ai')
# %%
def add_comments(board, batch_size=50):
    # Adds comment nodes
    query = '''
        // Create comments
        UNWIND $rows as comment
        MERGE (c:Comment {commentid: comment.Id, board: '%(board)s'})
        ON CREATE SET
        c.text = comment.Text

        // Match posts
        WITH comment, c
        MATCH (p:Post{postid: comment.PostId, board: '%(board)s'})
        MERGE (c)-[:RESPONDS_TO]->(p)

        // Match references
        WITH comment, c
        UNWIND comment.DOIs AS doi
        MATCH (r:Paper {doi:doi})
        MERGE (c)-[:CITES]->(r)
        RETURN count(c:Comment) as total
    ''' % dict(board=board)
    record_generator = iter_parse_stackexchange(board, 'Comments')
    return insert_data(conn, query, record_generator, batch_size)
# %%
def add_post_links(board, batch_size=50):
    # Adds post nodes
    query = '''
        // Create posts
        UNWIND $rows as post_link
        MATCH (p:Post {postid:post_link.PostId, board: '%(board)s'})
        MATCH (p2:Post {postid:post_link.RelatedPostId, board: '%(board)s'})

        MERGE (p)-[ref:REFERS_TO]->(p2)
        RETURN count(ref) as total
    ''' % dict(board=board)
    record_generator = iter_parse_stackexchange(board, 'PostLinks')
    return insert_data(conn, query, record_generator, batch_size)



# %%
se_boards = [
    "ai",
    "cstheory",
     "datascience",
     "stats",
     ]
#%%
add_boards( se_boards, batch_size=1)

# %%
for board in se_boards:
    print("Board:", board)
    add_posts(board, batch_size=100)
    add_comments(board, batch_size=100)
    add_post_links(board, batch_size=100)


# %%

def query_for_use_case_1(conn: Neo4jConnection, post_title: str):
   query = '''
    MATCH (post1:Post)
    WHERE post1.title CONTAINS "''' + post_title + '''"
    MATCH (post1)-[:CITES]->(paper:Paper)

    WITH paper, post1
    MATCH (post2: Post)-[:CITES]->(paper)
    RETURN post1
   '''
   return conn.query(query)
query_for_use_case_1(conn, 'backprop')

#%%
def query_for_use_case_2(conn: Neo4jConnection, post_title: str):
   query = '''
    MATCH (post1:Post)
    WHERE post1.title CONTAINS "''' + post_title + '''"
    MATCH (post1)-[:CITES]->(paper1:Paper)

    WITH paper1
    OPTIONAL MATCH (paper1)-[:CITES]->(paper2: Paper)
    OPTIONAL MATCH (author: Author)-[:AUTHORED]->(paper1)

    WITH author, paper1, paper2
    OPTIONAL MATCH (author)-[:AUTHORED]->(paper3: Paper)
    RETURN paper1, paper2, paper3
   '''
   return conn.query(query)

query_for_use_case_2(conn, 'backprop')

#%%
def query_for_use_case_3(conn: Neo4jConnection, paper_title: str):
   query = '''
    MATCH (paper:Paper)
    WHERE paper.title CONTAINS "''' + paper_title + '''"
    MATCH (post:Post)-[:CITES]->(paper:Paper)
    RETURN post
   '''
   return conn.query(query)
query_for_use_case_3(conn, 'backprop')

# %%
