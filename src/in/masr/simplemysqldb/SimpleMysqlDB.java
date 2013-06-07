package in.masr.simplemysqldb;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class SimpleMysqlDB {
	private String host;
	private String user;
	private String passwd;
	private String database;
	private String charset = "utf8";
	private int port = 3306;
	private int insertBufferSize = 500;
	private Connection con;
	private Map<Integer, BatchStmt> stmtMap;
	private int insertBatchIdIndex = 0;

	/**
	 * Look for <b>com.mysql.jdbc.Driver</b> in classpath and init variables to
	 * host, user, passwd and default database.
	 * 
	 * @param host
	 * @param user
	 * @param passwd
	 * @param database
	 * @throws MysqlInitException
	 */
	public SimpleMysqlDB(String host, String user, String passwd,
			String database) throws MysqlInitException {
		this.host = host;
		this.user = user;
		this.passwd = passwd;
		this.database = database;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException ex) {
			System.err
					.println("Can not find mysql-connector jdbc jar file. Please include it in classpath!");
			ex.printStackTrace();
			throw new MysqlInitException();
		}
		stmtMap = new HashMap<Integer, BatchStmt>();

	}

	/**
	 * Try to connect to mysql, establish session and logon. Usually, you should
	 * connect immediately after SimpleMysqlDB is created.
	 * 
	 * @throws SQLException
	 */
	public void connect() throws SQLException {
		/*
		 * This is a connection that used to do general jobs like simple update
		 * or insert or select, not fot batch insert
		 */
		con = createConnection();
		System.out.println("Mysql " + host + " is connected.");
	}

	private Connection createConnection() throws SQLException {
		Connection con;
		String url = "jdbc:mysql://" + host + ":" + port + "/" + database
				+ "?characterEncoding=" + charset;
		String username = user;
		String password = passwd;
		con = DriverManager.getConnection(url, username, password);
		return con;
	}

	public void closeConnection() throws SQLException {
		con.close();
	}

	/**
	 * Please do before method startInsertBatch if you want to set preferred
	 * insert_buffer_size value.
	 * 
	 * @param size
	 */
	public void setInsertBufferSize(int size) {
		System.out.println("Set mysql insert buffer size:" + size);
		this.insertBufferSize = size;
	}

	/**
	 * Please do before method connect if you want to set preferred mysql port
	 * value.
	 * 
	 * @param port
	 */
	public void setPort(int port) {
		System.out.println("Set mysql port:" + port);
		this.port = port;
	}

	/**
	 * Start a insert batch job. insert into mysql in batch mode.
	 * 
	 * @param table
	 *            the table name in mysql
	 * @param entityClass
	 *            the Entity Class that have EntityColumn above fields. Those
	 *            entity-column fields should have the same name with
	 *            corresponding mysql table column name and data type.
	 * @param delayCommit
	 *            If delaycommit is true, the insert batch job will not flush
	 *            the result to mysql until you do method endInsertBatch. If
	 *            delaycommit is false, the insert batch job will flush the
	 *            result to mysql immediately when batch's size exceed insert
	 *            buffer size.
	 * @return <b>batchId</b> an identifier that indicate the insert batch job.
	 *         It will be used when you try to insert record or finish the batch
	 *         job.
	 * @throws SQLException
	 */
	@SuppressWarnings("rawtypes")
	public int startInsertBatch(String table, Class entityClass,
			boolean delayCommit) throws SQLException {

		StringBuilder sql = new StringBuilder();
		// generate insert sql
		sql.append("insert into " + table + " ");
		Field[] fileds = entityClass.getFields();
		sql.append("(");
		int count = 0;
		for (Field field : fileds) {
			if (field.isAnnotationPresent(EntityColumn.class)) {
				count++;
				String fieldName = field.getName();
				sql.append(fieldName + ",");
			}
		}
		sql = new StringBuilder(sql.substring(0, sql.length() - 1));
		sql.append(") values(");
		for (int i = 0; i < count; i++) {
			sql.append("?,");
		}
		sql = new StringBuilder(sql.substring(0, sql.length() - 1));
		sql.append(")");

		// create a new connection for each batch insert, because auto
		// commit can only combine with connection
		// Different batch insert may have different auto commit value
		Connection connection = createConnection();
		PreparedStatement stmt = connection.prepareStatement(sql.toString());
		BatchStmt batchStmt = new BatchStmt(stmt, entityClass, table,
				delayCommit);
		insertBatchIdIndex++;
		stmtMap.put(insertBatchIdIndex, batchStmt);

		System.out.println("Mysql started insert batch: " + batchStmt.name);

		return insertBatchIdIndex;
	}

	/**
	 * Start a insert batch job. insert into mysql in batch mode.
	 * 
	 * @param table
	 *            the table name in mysql
	 * @param entityClass
	 *            the Entity Class that have EntityColumn above fields. Those
	 *            entity-column fields should have the same name with
	 *            corresponding mysql table column name and data type.
	 * @return <b>batchId</b> an identifier that indicate the insert batch job.
	 *         It will be used when you try to insert record or finish the batch
	 *         job.
	 * @throws SQLException
	 */
	@SuppressWarnings("rawtypes")
	public int startInsertBatch(String table, Class entityClass)
			throws Exception {
		return startInsertBatch(table, entityClass, false);
	}

	/**
	 * Insert a record using a batchId. Should do method startInsertBatch before
	 * do insert. Please offer the right batchId according to the entity.
	 * 
	 * @param entity
	 *            the record you want to insert.
	 * @param batchId
	 *            the value return from startInsertBatch.
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public void insert(Object entity, int batchId) throws Exception {

		BatchStmt bs = stmtMap.get(batchId);
		Class entityClass = bs.entityClass;
		Field[] fileds = entityClass.getFields();
		int count = 0;
		for (Field field : fileds) {
			if (field.isAnnotationPresent(EntityColumn.class)) {
				count++;
				Class fieldType = field.getType();
				int lastDot = fieldType.getName().lastIndexOf('.');
				String fieldTypeName = fieldType.getName().substring(
						lastDot + 1);
				fieldTypeName = fieldTypeName.substring(0, 1).toUpperCase()
						+ fieldTypeName.substring(1);
				Method method = bs.pStatement.getClass().getMethod(
						"set" + fieldTypeName,
						new Class[] { int.class, fieldType });

				Object[] args = { count, field.get(entity) };
				method.invoke(bs.pStatement, args);
			}
		}
		bs.buffer++;
		bs.total++;
		bs.pStatement.addBatch();
		if (bs.buffer > insertBufferSize && !bs.delaySubmit) {
			bs.pStatement.executeBatch();
			bs.buffer = 0;
			System.out.println("Mysql insert batch:" + bs.name
					+ "Already inserted " + bs.total);
		}
	}

	/**
	 * Always do this when you finish a batch insert job. It will do some clean
	 * job.
	 * 
	 * @param batchId
	 * @throws SQLException
	 */
	public void endInsertBatch(int batchId) throws SQLException {
		BatchStmt batchStmt = this.stmtMap.get(batchId);
		batchStmt.pStatement.executeBatch();
		System.out.println("Mysql insert batch:" + batchStmt.name
				+ "Totally inserted " + batchStmt.total);
		stmtMap.remove(batchId);
		System.out.println("Mysql insert batch ended:" + batchStmt.name);
	}

	/**
	 * You can use a querytext that contains <b>"?"</b> in it. We will replace
	 * <b>"?"</b> by params in sequence one by one. It is very useful when you
	 * want to build dynamic SQL.<br/>
	 * <b>Note</b> : If your SQL is like <b>select * from A where str =
	 * 'haha?'</b> and in fact you do not want the replacement happen, this
	 * method will not work!!!
	 * 
	 * @param querytext
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public void executeByTemplateSQL(String querytext, String[] params)
			throws Exception {
		boolean canbreak = false;
		int i = 0;
		while (!canbreak) {
			if (querytext.indexOf('?') != -1 && i < params.length) {
				querytext = querytext.replaceFirst("\\?", params[i]);
				i++;
			} else {
				canbreak = true;
			}
		}
		executeSQL(querytext);
	}

	public void executeSQL(String sql) throws SQLException {
		Statement stm = con.createStatement();
		System.out.println("Mysql executing sql:\n" + sql);
		int rows = stm.executeUpdate(sql);
		System.out.println("Affected rows : " + rows);

	}

	public ResultSet getResultSet(String sql) throws SQLException {
		Statement stmt = con.createStatement();
		System.out.println("Mysql fetching data: " + sql);
		ResultSet set = stmt.executeQuery(sql);
		return set;

	}

	private class BatchStmt {
		public PreparedStatement pStatement;
		@SuppressWarnings("rawtypes")
		public Class entityClass;
		public int buffer = 0;
		public int total = 0;
		public boolean delaySubmit;
		public String name;

		@SuppressWarnings("rawtypes")
		public BatchStmt(PreparedStatement stmt, Class c, String table,
				boolean autoCommit) throws SQLException {
			this.pStatement = stmt;
			this.entityClass = c;
			this.delaySubmit = autoCommit;
			this.name = table + "(" + c.getSimpleName() + ")";
		}
	}

}
