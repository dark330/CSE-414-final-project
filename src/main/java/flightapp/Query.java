package flightapp;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.security.*;
import java.security.spec.*;
import javax.crypto.*;
import javax.crypto.spec.*;

/**
 * Runs queries against a back-end database
 */
public class Query {
	// DB Connection
	private Connection conn;

	// Password hashing parameter constants
	private static final int HASH_STRENGTH = 65536;
	private static final int KEY_LENGTH = 128;

	private String loggedInUserName = null;

	// Canned queries
	private static final String CHECK_FLIGHT_CAPACITY = "SELECT * FROM Flights WHERE fid = ?";
	private PreparedStatement checkFlightCapacityStatement;

	// For check dangling
	private static final String TRANCOUNT_SQL = "SELECT @@TRANCOUNT AS tran_count";
	private PreparedStatement tranCountStatement;

	private static final String BASE_DELETE_STATMENT = "DELETE FROM ";
	private static final String TRUNCATE_RESERVATION = "TRUNCATE TABLE RESERVATIONS";
	private PreparedStatement truncateReservation;
	private static final String[] TABLE_NAMES = {"RESERVATIONS", "USERS"};

	private PreparedStatement[] resetTables;

	private PreparedStatement indirectQuery;
	private PreparedStatement searchDirectFlightStatement;
	private static final String DIRECT_FLIGHT_STATEMENT = "SELECT DISTINCT TOP (?) *"
		+ "FROM Flights " + "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND canceled = 0 " +
		"ORDER BY actual_time ASC;";

	private static final String CONNECT_FLIGHT_STATEMENT = "SELECT DISTINCT TOP (?) F.day_of_month 'day_of_month1'," +
		"F.carrier_id as carrier_id1," +
		"F.flight_num as flight_num1," +
		"F.origin_city as origin_city1," +
		"F.dest_city as dest_city1," +
		"F.actual_time as actual_time1," +
		"F.capacity as capacity1, F.price as price1," +
		"F.fid as fid1, G.fid as fid2, " +
		"G.day_of_month as day_of_month2," +
		"G.carrier_id as carrier_id2," +
		"G.flight_num as flight_num2," +
		"G.origin_city as origin_city2," +
		"G.dest_city as dest_city2," +
		"G.actual_time as actual_time2," +
		"G.capacity as capacity2, G.price as price2," +
		"(F.price+G.price) as total_price, (F.actual_time+G.actual_time) as total_time " +
		"FROM Flights AS F left join Flights AS G ON  F.dest_city = G.origin_city " +
		"WHERE F.origin_city = ? AND G.dest_city = ? AND F.canceled = 0" +
		" AND G.canceled = 0 AND F.day_of_month = G.day_of_month AND F.day_of_month = ? " +
		"ORDER BY total_time ASC;";

	private static final String GET_FLIGHT_PRICE = "SELECT price FROM Flights WHERE fid = ?;";
	private PreparedStatement getFlightPrice;

	private Map<Integer, List<Flight[]>> searchResult = new TreeMap<>();
	private Map<Integer, Flight[]> itinResult = new TreeMap<>();
	private int itinIdMax = 0;
	// Get user name
	private static final String GET_USER = "SELECT password_hash, password_salt FROM USERS WHERE username = ?;";
	private PreparedStatement getUserStatement;

	private static final String GET_USER_BALANCE = "SELECT balance FROM USERS WHERE username = ?;";
	private PreparedStatement getUserBalance;

	// Insert new user info into database
	private static final String CREATE_USER = "INSERT INTO USERS VALUES (?,?,?,?);";
	private PreparedStatement createUserStatement;

	// get reservation flights
	private static final String GET_UNPAID_FLIGHT = "SELECT * " +
		"FROM RESERVATIONS WHERE Rid = ? AND username = ? AND paid = 0;";
	private PreparedStatement getUnpaidFlights;

	// get reservation flights
	private static final String GET_RESERVED_FLIGHT_BY_DATE = "SELECT * " +
		"FROM RESERVATIONS WHERE date = ? AND username = ?;";
	private PreparedStatement getReservedFlightsByDate;

	// insert reservation
	//private static final String INSERT_RESERVATION = "INSERT INTO RESERVATIONS VALUES (?,?,?,?,?,?,?);";
	private static final String INSERT_RESERVATION = "INSERT INTO RESERVATIONS (username, paid, canceled, date, fid1, fid2)" +
													 " VALUES (?,?,?,?,?,?);";
	private PreparedStatement insertReservation;

	// check if username and fid exist in reservation
	private static final String GET_RESERVATION = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SELECT * FROM RESERVATIONS WHERE username = ? AND fid1 = ?;";
	private static final String GET_RESERVATION_BY_ID = /*"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;*/ "SELECT * FROM RESERVATIONS WHERE username = ? AND Rid = ?;";
	private static final String CANCEL_RESERVATION = "UPDATE RESERVATIONS SET canceled = 1 WHERE Rid = ?;";
	private PreparedStatement cancelReservation;
	private PreparedStatement getReservation;
	private PreparedStatement getReservationById;


	// get count in reservation flights for certain fid
	private static final String RESERVED_SEATS_TOTAL = /*"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; "+*/"SELECT COUNT(*) AS count " +
		"FROM RESERVATIONS " + "WITH (TABLOCKX, HOLDLOCK) " +
		"WHERE canceled = 0 AND (fid1 = ? OR fid2 = ?);";
	private PreparedStatement totalReservedSeatsNum;

	private static final String UPDATE_RESERVATION_PAID = "UPDATE RESERVATIONS SET paid = 1 WHERE username = ?;";
	private PreparedStatement updateReservationPaid;

	private static final String UPDATE_USER_BALANCE = "UPDATE USERS SET balance = ? WHERE username = ?;";
	private PreparedStatement updateUserBalance;

	// ========================= reservation methods =========================
	// Get reservation flights information for reservation listing
	private static final String GET_RESERVATIONS_AND_ALL_FLIGHTS = "SELECT * FROM RESERVATIONS AS R, FLIGHTS AS F " +
																   "WHERE R.fid1 = F.fid AND R.Rid = ? " + "UNION ALL " +
																   "SELECT * FROM RESERVATIONS AS R, FLIGHTS AS F " +
																   "WHERE R.fid2 = F.fid AND R.Rid = ? " + "ORDER BY R.Rid;";
	private PreparedStatement getReservationAndAllFlights;

	private static final String GET_RESERVATION_BY_NAME = "SELECT * FROM RESERVATIONS WHERE username = ? ORDER BY Rid;";
	private PreparedStatement getReservationByName;

	private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;

	public Query() throws SQLException, IOException {
		this(null, null, null, null);
	}

	protected Query(String serverURL, String dbName, String adminName, String password)
		throws SQLException, IOException {
		conn = serverURL == null ? openConnectionFromDbConn()
			: openConnectionFromCredential(serverURL, dbName, adminName, password);

		prepareStatements();
	}

	/**
	 * Return a connecion by using dbconn.properties file
	 *
	 * @throws SQLException
	 * @throws IOException
	 */
	public static Connection openConnectionFromDbConn() throws SQLException, IOException {
		// Connect to the database with the provided connection configuration
		Properties configProps = new Properties();
		configProps.load(new FileInputStream("dbconn.properties"));
		String serverURL = configProps.getProperty("flightapp.server_url");
		String dbName = configProps.getProperty("flightapp.database_name");
		String adminName = configProps.getProperty("flightapp.username");
		String password = configProps.getProperty("flightapp.password");
		return openConnectionFromCredential(serverURL, dbName, adminName, password);
	}

	/**
	 * Return a connecion by using the provided parameter.
	 *
	 * @param serverURL example: example.database.widows.net
	 * @param dbName    database name
	 * @param adminName username to login server
	 * @param password  password to login server
	 * @throws SQLException
	 */
	protected static Connection openConnectionFromCredential(String serverURL, String dbName,
	                                                         String adminName, String password) throws SQLException {
		String connectionUrl =
			String.format("jdbc:sqlserver://%s:1433;databaseName=%s;user=%s;password=%s", serverURL,
				dbName, adminName, password);
		Connection conn = DriverManager.getConnection(connectionUrl);
		conn.setNetworkTimeout(null, 30000); //I don't have an Executor, so the field is set to null


		// By default, automatically commit after each statement
		conn.setAutoCommit(true);

		// By default, set the transaction isolation level to serializable
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

		return conn;
	}

	/**
	 * Get underlying connection
	 */
	public Connection getConnection() {
		return conn;
	}

	/**
	 * Closes the application-to-database connection
	 */
	public void closeConnection() throws SQLException {
		conn.close();
	}

	/**
	 * Clear the data in any custom tables created.
	 * <p>
	 * WARNING! Do not drop any tables and do not clear the flights table.
	 */
	public void clearTables() {
		try {
			truncateReservation.executeUpdate();
			beginTransaction();
			for (PreparedStatement reset : resetTables) {
				reset.executeUpdate();
			}
			commitTransaction();
		} catch (SQLException e) {
			try {
				rollbackTransaction();
				String msg = e.getMessage();
				e.printStackTrace();
			} catch(SQLException e2) {
				e2.printStackTrace();
			}
		}
	}

	/*
	 * prepare all the SQL statements in this method.
	 */
	private void prepareStatements() throws SQLException {
		checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
		tranCountStatement = conn.prepareStatement(TRANCOUNT_SQL);
		resetTables = new PreparedStatement[TABLE_NAMES.length];
		for (int i = 0; i < TABLE_NAMES.length; i++) {
			resetTables[i] = conn.prepareStatement(BASE_DELETE_STATMENT + TABLE_NAMES[i]);
		}
		truncateReservation = conn.prepareStatement(TRUNCATE_RESERVATION);
		searchDirectFlightStatement = conn.prepareStatement(DIRECT_FLIGHT_STATEMENT);
		indirectQuery = conn.prepareStatement(CONNECT_FLIGHT_STATEMENT);

		getUserStatement = conn.prepareStatement(GET_USER);
		createUserStatement = conn.prepareStatement(CREATE_USER);

		getUserBalance = conn.prepareStatement(GET_USER_BALANCE);
		getFlightPrice = conn.prepareStatement(GET_FLIGHT_PRICE);

		getUnpaidFlights = conn.prepareStatement(GET_UNPAID_FLIGHT);
		getReservedFlightsByDate = conn.prepareStatement(GET_RESERVED_FLIGHT_BY_DATE);
		insertReservation = conn.prepareStatement(INSERT_RESERVATION);
		updateReservationPaid = conn.prepareStatement(UPDATE_RESERVATION_PAID);
		updateUserBalance = conn.prepareStatement(UPDATE_USER_BALANCE);
		totalReservedSeatsNum = conn.prepareStatement(RESERVED_SEATS_TOTAL);
		getReservation = conn.prepareStatement(GET_RESERVATION);
		getReservationAndAllFlights = conn.prepareStatement(GET_RESERVATIONS_AND_ALL_FLIGHTS);
		getReservationByName = conn.prepareStatement(GET_RESERVATION_BY_NAME);
		getReservationById = conn.prepareStatement(GET_RESERVATION_BY_ID);
		cancelReservation = conn.prepareStatement(CANCEL_RESERVATION);

		beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);
	}

	/**
	 * Takes a user's username and password and attempts to log the user in.
	 *
	 * @param username user's username
	 * @param password user's password
	 * @return If someone has already logged in, then return "User already logged in\n" For all other
	 * errors, return "Login failed\n". Otherwise, return "Logged in as [username]\n".
	 */
	public String transaction_login(String username, String password) {
		try {
			username = username.toLowerCase();
			if (this.loggedInUserName != null) {
				return "User already logged in\n";
			}
			getUserStatement.clearParameters();
			getUserStatement.setString(1, username);
			ResultSet getResUser = getUserStatement.executeQuery();

			if (getResUser.next()) { // username exists in database
				this.loggedInUserName = username;
				byte[] hash1 = getResUser.getBytes("password_hash");
				byte[] salt = getResUser.getBytes("password_salt");
				KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, HASH_STRENGTH, KEY_LENGTH);
				SecretKeyFactory factory = null;
				byte[] hash2 = null;
				try {
					factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
					hash2 = factory.generateSecret(spec).getEncoded();
				} catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
					throw new IllegalStateException();
				}
				if (Arrays.equals(hash1, hash2)) {
					getResUser.close();
					return "Logged in as " + this.loggedInUserName + "\n";
				}
			}

			getResUser.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			checkDanglingTransaction();
		}
		return "Login failed\n";
	}

	/**
	 * Implement the create user function.
	 *
	 * @param username   new user's username. User names are unique the system.
	 * @param password   new user's password.
	 * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure
	 *                   otherwise).
	 * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
	 */
	public String transaction_createCustomer(String username, String password, int initAmount) {
		try {
			if (initAmount < 0) {
				return "Failed to create user\n";
			}

			// Generate a random cryptographic salt
			SecureRandom random = new SecureRandom();
			byte[] salt = new byte[16];
			random.nextBytes(salt);

			// Specify the hash parameters
			KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, HASH_STRENGTH, KEY_LENGTH);

			// Generate the hash
			SecretKeyFactory factory = null;
			byte[] hash = null;
			try {
				factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
				hash = factory.generateSecret(spec).getEncoded();
			} catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
				throw new IllegalStateException();
			}

			username = username.toLowerCase();
			getUserStatement.clearParameters();
			getUserStatement.setString(1, username);
			ResultSet getRes = getUserStatement.executeQuery();
			//THERE ALREADY EXISTS A USER W SAME USERNAME
			beginTransaction();
			if (getRes.next()) {
				getRes.close();
				rollbackTransaction();
				return "Failed to create user\n";
			}
			getRes.close();
			createUserStatement.clearParameters();
			createUserStatement.setString(1, username);
			createUserStatement.setBytes(2, hash);
			createUserStatement.setBytes(3, salt);
			createUserStatement.setInt(4, initAmount);
			createUserStatement.executeUpdate();
			commitTransaction();
			return "Created user " + username + "\n";

		} catch (SQLException e) {
			try {
				e.printStackTrace();
				rollbackTransaction();
			} catch (SQLException e2) {
				e2.printStackTrace();
			}
		} finally {
			checkDanglingTransaction();
		}

		return "Failed to create user\n";
	}

	/**
	 * Implement the search function.
	 * <p>
	 * Searches for flights from the given origin city to the given destination city, on the given day
	 * of the month. If {@code directFlight} is true, it only searches for direct flights, otherwise
	 * is searches for direct flights and flights with two "hops." Only searches for up to the number
	 * of itineraries given by {@code numberOfItineraries}.
	 * <p>
	 * The results are sorted based on total flight time.
	 *
	 * @param originCity
	 * @param destinationCity
	 * @param directFlight        if true, then only search for direct flights, otherwise include
	 *                            indirect flights as well
	 * @param dayOfMonth
	 * @param numberOfItineraries number of itineraries to return
	 * @return If no itineraries were found, return "No flights match your selection\n". If an error
	 * occurs, then return "Failed to search\n".
	 * <p>
	 * Otherwise, the sorted itineraries printed in the following format:
	 * <p>
	 * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time]
	 * minutes\n [first flight in itinerary]\n ... [last flight in itinerary]\n
	 * <p>
	 * Each flight should be printed using the same format as in the {@code Flight} class.
	 * Itinerary numbers in each search should always start from 0 and increase by 1.
	 * @see Flight#toString()
	 */
	public String transaction_search(String originCity, String destinationCity, boolean directFlight,
	                                 int dayOfMonth, int numberOfItineraries) {
		searchResult.clear();
		itinIdMax = 0;
		StringBuffer sb = new StringBuffer();
		int count = 0;
		try {
			count = direct_search(originCity, destinationCity, dayOfMonth, numberOfItineraries);
			if (searchResult.isEmpty()) {
				return "No flights match your selection\n";
			}
			if (!directFlight && count < numberOfItineraries) {
				connect_search(originCity, destinationCity, dayOfMonth, numberOfItineraries - count);
			}
			count = 0;
			for (Integer i : searchResult.keySet()) {
				List<Flight[]> list = searchResult.get(i);
				for (Flight[] arr : list) {
					if (arr.length < 2) {
						sb.append("Itinerary " + count + ": 1 flight(s), " + i + " minutes\n");
						sb.append(arr[0].toString() + "\n");
					} else {
						sb.append("Itinerary " + count + ": 2 flight(s), " + i + " minutes\n");
						sb.append(arr[0].toString() + "\n");
						sb.append(arr[1].toString() + "\n");
					}
					itinResult.put(count, arr);
					count++;
				}
			}

		} catch (IllegalStateException e) {
			return "Failed to search\n";
		}
		itinIdMax = count;
		return sb.toString();
	}

	/**
	 * Searches for direct flights from the given origin city to the given destination city
	 * on the given day of the month. Up to "count" number of flights are chosen.
	 *
	 * @param oCity origin City of the flight
	 * @param dCity destination City of the flight
	 * @param date  day of the month
	 * @param count maximum number of flights to be included in the result
	 * @return Returns an integer representing the number of elements in the result
	 * If anything goes wrong in the query process, an IllegalStateException is thrown
	 */
	public Integer direct_search(String oCity, String dCity, int date, int count) {
		int count2 = 0;
		ResultSet result;
		List<Flight[]> direct_flights = new ArrayList<>();
		try {
			searchDirectFlightStatement.clearParameters();
			searchDirectFlightStatement.setInt(1, count);
			searchDirectFlightStatement.setString(2, oCity);
			searchDirectFlightStatement.setString(3, dCity);
			searchDirectFlightStatement.setInt(4, date);
			result = searchDirectFlightStatement.executeQuery();

			while (result.next()) {
				Flight temp = new Flight();
				Flight[] arr = new Flight[1];
				temp.fid = result.getInt("fid");
				temp.dayOfMonth = result.getInt("day_of_month");
				temp.carrierId = result.getString("carrier_id");
				temp.flightNum = result.getString("flight_num");
				temp.originCity = result.getString("origin_city");
				temp.destCity = result.getString("dest_city");
				temp.time = result.getInt("actual_time");
				temp.capacity = result.getInt("capacity");
				temp.price = result.getInt("price");
				arr[0] = temp;
				if (searchResult.containsKey(temp.time)) {
					List<Flight[]> list = searchResult.get(temp.time);
					int index = list.size();
					for(int i = 0; i < list.size(); i++) {
						Flight [] flights = list.get(i);
						int fid1 = flights[0].fid;
						if (flights.length < 2 && fid1 > temp.fid) {
							index = i;
						}
					}
					list.add(index, arr);
					searchResult.put(temp.time, list);
				} else {
					List<Flight[]> list = new ArrayList<>();
					list.add(arr);
					searchResult.put(temp.time, list);
				}
				count2++;
			}
			result.close();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException();
		}
		return count2;
	}

	/**
	 * Searches for connect flights from the given origin city to the given destination city
	 * on the given day of the month. Up to "count" number of flights are chosen
	 *
	 * @param oCity origin City of the flight
	 * @param dCity destination City of the flight
	 * @param date  day of the month
	 * @param count maximum number of flights to be included in the result
	 *              <p>
	 *              If anything goes wrong in the query process, an IllegalStateException is thrown
	 */
	public void connect_search(String oCity, String dCity, int date, int count) {
		ResultSet result;
		try {
			indirectQuery.clearParameters();
			indirectQuery.setInt(1, count);
			indirectQuery.setString(2, oCity);
			indirectQuery.setString(3, dCity);
			indirectQuery.setInt(4, date);
			result = indirectQuery.executeQuery();
			while (result.next()) {
				Flight[] arr = new Flight[2];
				Flight f1 = new Flight();
				f1.fid = result.getInt("fid1");
				f1.dayOfMonth = result.getInt("day_of_month1");
				f1.carrierId = result.getString("carrier_id1");
				f1.flightNum = result.getString("flight_num1");
				f1.originCity = result.getString("origin_city1");
				f1.destCity = result.getString("dest_city1");
				f1.time = result.getInt("actual_time1");
				f1.capacity = result.getInt("capacity1");
				f1.price = result.getInt("price1");
				Flight f2 = new Flight();
				f2.fid = result.getInt("fid2");
				f2.dayOfMonth = result.getInt("day_of_month2");
				f2.carrierId = result.getString("carrier_id2");
				f2.flightNum = result.getString("flight_num2");
				f2.originCity = result.getString("origin_city2");
				f2.destCity = result.getString("dest_city2");
				f2.time = result.getInt("actual_time2");
				f2.capacity = result.getInt("capacity2");
				f2.price = result.getInt("price2");
				arr[0] = f1;
				arr[1] = f2;
				if (searchResult.containsKey(f1.time + f2.time)) {
					List<Flight[]> list = searchResult.get(f1.time + f2.time);
					list.add(arr);
					searchResult.put(f1.time + f2.time, list);
				} else {
					List<Flight[]> list = new ArrayList<>();
					list.add(arr);
					searchResult.put(f1.time + f2.time, list);
				}
			}
			result.close();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException();
		}
	}

	/**
	 * Implements the book itinerary function.
	 *
	 * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in
	 *                    the current session.
	 * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
	 * If the user is trying to book an itinerary with an invalid ID or without having done a
	 * search, then return "No such itinerary {@code itineraryId}\n". If the user already has
	 * a reservation on the same day as the one that they are trying to book now, then return
	 * "You cannot book two flights in the same day\n". For all other errors, return "Booking
	 * failed\n".
	 * <p>
	 * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n"
	 * where reservationId is a unique number in the reservation system that starts from 1 and
	 * increments by 1 each time a successful reservation is made by any user in the system.
	 */

	public String transaction_book(int itineraryId) {
		// check if logged in
		boolean isDeadLock;
		if (loggedInUserName == null) {
			return "Cannot book reservations, not logged in\n";
		}
		// check if the search id is actually in the itn flights table
		if (itineraryId >= 0 && itineraryId <= itinIdMax && !itinResult.isEmpty()) {
			Flight[] arr = itinResult.get(itineraryId);
			int dateInItn = arr[0].dayOfMonth;
			if (sameDayFlightsInReservation(loggedInUserName, dateInItn)) {
				return "You cannot book two flights in the same day\n";
			}
			// insert into reservation
			int fid1 = arr[0].fid;
			int fid2 = -1;
			if (arr.length > 1) {
				fid2 = arr[1].fid;
			}
			int reservationId = itineraryId;
			int retries = 3;
			ResultSet reservationResult;
				do {
					isDeadLock = false;
					try {
						getReservation.clearParameters();
						getReservation.setString(1, loggedInUserName);
						getReservation.setInt(2, fid1);
						beginTransaction();
						reservationResult = getReservation.executeQuery();
						if (reservationResult.next()) {
							reservationResult.close();
							System.out.println("No reservation coresponding to username and fid!");
							try {
								rollbackTransaction();
							} catch (SQLException e) {
							}
							return "Booking failed\n";
						}
						for (Flight f : arr) {
							int fid = f.fid;
							// check if seats available for this flight
							try {
								if (!seatsAvailable(fid)) {
									System.out.println("No seats available!");
									rollbackTransaction();
									return "Booking failed\n";
								}
							} catch (IllegalStateException e) {
								rollbackTransaction();
								String msg = e.getMessage();
								throw new IllegalStateException("check flight capacity failed!" + msg + "\n");
							}
						}
						reservationResult.close(); // close previous read query.
						insertReservation.clearParameters();
						insertReservation.setString(1, loggedInUserName);
						insertReservation.setInt(2, 0);
						insertReservation.setInt(3, 0);
						insertReservation.setInt(4, dateInItn);
						insertReservation.setInt(5, fid1);
						insertReservation.setInt(6, fid2);
						insertReservation.executeUpdate();
						commitTransaction();
					} catch (SQLException e) {
						String erMsg = e.getMessage();
						try {
							e.printStackTrace();
							System.out.println("Insert reservation failed, " + erMsg + "!");
							if (isDeadLock(e)) {
								isDeadLock = true;
								System.out.println("isDeadLock: " + isDeadLock);
							} else {
								rollbackTransaction();
							}
						} catch (SQLException e3) {
							String erMsg3 = e3.getMessage();
							System.out.println("Rollback failed, " + erMsg3 + "!");
						}
						return "Booking failed\n";
					}

			} while (isDeadLock && retries-- > 0);
				try {
					reservationResult = getReservation.executeQuery();
					reservationResult.next();
					reservationId = reservationResult.getInt("Rid");
					reservationResult.close();
					return "Booked flight(s), reservation ID: " + reservationId + "\n";
				} catch(SQLException e) {
					e.printStackTrace();
				}
			}

		return String.format("No such itinerary %s\n", itineraryId);
	}



	/**
	 * Check if seats available for this flight
	 *
	 * @param fid - int - the flight id that we want to check whether seats available
	 * @return - Boolean
	 */
	private boolean seatsAvailable(int fid) {
		int currentCapacity;
		try {
			currentCapacity = checkFlightCapacity(fid);
		} catch (IllegalStateException e) {
			throw new IllegalStateException("check flight capacity failed!\n");
		}
		// Check count in the reservation flight
		try {
			totalReservedSeatsNum.clearParameters();
			totalReservedSeatsNum.setInt(1, fid);
			totalReservedSeatsNum.setInt(2, fid);
			ResultSet reservedNumResult = totalReservedSeatsNum.executeQuery();

			if (reservedNumResult.next()) {
				int reservedNum = reservedNumResult.getInt("count");
				int availabeSeatsNum = currentCapacity - reservedNum;
				reservedNumResult.close();
				if (availabeSeatsNum > 0) {
					return true;
				}
				return false;
			}
			return true;

		} catch (SQLException e) {
			String erMsg = e.getMessage();
			throw new IllegalStateException("Cannot get available seats, " + erMsg + "!\n");
		}
	}

	/**
	 * Check if we had same day flights, if user want to book an intnerary in the same day as reservations
	 *
	 * @param ItinId - Itnerary id which is used to search for flights
	 * @return boolean - if same day, return true, if not return false
	 */
	private boolean sameDayFlightsInReservation(String username, int dateInItn) {
		// Get reserved flights based on date in itner flights
		ResultSet reservedFlights = null;
		boolean existInReserveTable;
		try {
			getReservedFlightsByDate.clearParameters();
			getReservedFlightsByDate.setInt(1, dateInItn);
			getReservedFlightsByDate.setString(2, username);
			reservedFlights = getReservedFlightsByDate.executeQuery();
			// if the same date exists in the reserved fligths table
			existInReserveTable = reservedFlights.next();
			reservedFlights.close();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException("Get reserved Flightes failed!\n");
		}
		return existInReserveTable;
	}

	/**
	 * Implements the pay function.
	 *
	 * @param reservationId the reservation to pay for.
	 * @return If no user has logged in, then return "Cannot pay, not logged in\n" If the reservation
	 * is not found / not under the logged in user's name, then return "Cannot find unpaid
	 * reservation [reservationId] under user: [username]\n" If the user does not have enough
	 * money in their account, then return "User has only [balance] in account but itinerary
	 * costs [cost]\n" For all other errors, return "Failed to pay for reservation
	 * [reservationId]\n"
	 * <p>
	 * If successful, return "Paid reservation: [reservationId] remaining balance:
	 * [balance]\n" where [balance] is the remaining balance in the user's account.
	 */
	public String transaction_pay(int reservationId) {
		int balance = 0;
		int price;
		ResultSet userResult;
		ResultSet unpaidFlights;
		if (loggedInUserName == null) {
			return "Cannot pay, not logged in\n";
		}
		try {
			getUnpaidFlights.clearParameters();
			getUnpaidFlights.setInt(1, reservationId);
			getUnpaidFlights.setString(2, loggedInUserName);
			unpaidFlights = getUnpaidFlights.executeQuery();
			if (unpaidFlights.next()) {
				try {
					getUserBalance.clearParameters();
					getUserBalance.setString(1, loggedInUserName);
					userResult = getUserBalance.executeQuery();
				} catch (SQLException e3) {
					String erMsg3 = e3.getMessage();
					throw new IllegalStateException("getUserBalance failed, " + erMsg3 + "!\n");
				}

				if (userResult.next()) {
					balance = userResult.getInt("balance");
				}
				userResult.close();
				int fid1 = unpaidFlights.getInt("fid1");
				price = getPrice(fid1);
				int fid2 = unpaidFlights.getInt("fid2");
				// add second flight if indirect flight
				if (fid2 != -1) {
					price += getPrice(fid2);
				}
				// check if enough balance
				if (balance >= price) {
					// set paid in reservation table as 1
					try {
						beginTransaction();
						updateReservationPaid.clearParameters();
						updateReservationPaid.setString(1, loggedInUserName);
						updateReservationPaid.executeUpdate();
					} catch(SQLException e){
						String erMsg = e.getMessage();
						try {
							rollbackTransaction();
							e.printStackTrace();
							throw new IllegalStateException("Update reservation paid state faild, "+ erMsg +"!\n");
						} catch (SQLException e2) {
							String erMsg2 = e2.getMessage();
							throw new IllegalStateException("Rollback failed, " + erMsg2 + "!\n");
						}
					}

					// subtract user balance
					try {
						updateUserBalance.clearParameters();
						updateUserBalance.setInt(1, balance - price);
						updateUserBalance.setString(2, loggedInUserName);
						updateUserBalance.executeUpdate();
						commitTransaction();
						unpaidFlights.close();
						return "Paid reservation: " + reservationId + " remaining balance: " + (balance-price) + "\n";
					} catch(SQLException e){
						String erMsg = e.getMessage();
						try {
							rollbackTransaction();
							e.printStackTrace();
							return "Failed to pay for reservation " + reservationId + "\n";
						} catch (SQLException e2) {
							String erMsg2 = e2.getMessage();
							throw new IllegalStateException("Rollback failed, " + erMsg2 + "!\n");
						}
					}
				} else {
					unpaidFlights.close();
					return "User has only " + balance + " in account but itinerary costs " + price + "\n";
				}
			}
			unpaidFlights.close();
			return "Cannot find unpaid reservation " + reservationId + " under user: " + loggedInUserName +"\n";
		} catch (SQLException e) {
			e.printStackTrace();
			return "Failed to pay for reservation " + reservationId + "\n";
		}
	}

	/**
	 * Helper function
	 * @param fid flight ID
	 * @return returns the price of the given flightID
	 *
	 * */
	private int getPrice(int fid) throws SQLException {
		if (fid == -1) {
			return 0;
		}
		ResultSet result;
		getFlightPrice.clearParameters();
		getFlightPrice.setInt(1,fid);
		try {
			result = getFlightPrice.executeQuery();
			result.next();
		} catch(SQLException e) {
			return getPrice(fid);
		}
		int price = result.getInt("price");
		result.close();
		return price;
	}

	/**
	 * Implements the reservations function.
	 *
	 * @return If no user has logged in, then return "Cannot view reservations, not logged in\n" If
	 * the user has no reservations, then return "No reservations found\n" For all other
	 * errors, return "Failed to retrieve reservations\n"
	 * <p>
	 * Otherwise return the reservations in the following format:
	 * <p>
	 * Reservation [reservation ID] paid: [true or false]:\n [flight 1 under the
	 * reservation]\n [flight 2 under the reservation]\n Reservation [reservation ID] paid:
	 * [true or false]:\n [flight 1 under the reservation]\n [flight 2 under the
	 * reservation]\n ...
	 * <p>
	 * Each flight should be printed using the same format as in the {@code Flight} class.
	 * @see Flight#toString()
	 */
	public String transaction_reservations() {
		try {
			// Check if logged in
			if (loggedInUserName == null) {
				return "Cannot view reservations, not logged in\n";
			}
			StringBuffer sb = new StringBuffer();
			ResultSet getReservationByNameResult;
			ResultSet getReservationAndAllFlightsResult;
			// Get reservation by username
			try {
				getReservationByName.clearParameters();
				getReservationByName.setString(1, loggedInUserName);
				getReservationByNameResult = getReservationByName.executeQuery();
			} catch (SQLException e) {
				e.printStackTrace();
				System.out.println("SQL getReservationAndAllFlights failed!" + e.getMessage()+ "\n");
				return "Failed to retrieve reservations\n";
			}
			// Check is result is empty
			if (!getReservationByNameResult.isBeforeFirst() ) {
				return "No reservations found\n";
			}
			while (getReservationByNameResult.next()) {
				int Rid = getReservationByNameResult.getInt("Rid");
				int Rid_prev = -1111;

				// Get flights according to Rid
				try {
					getReservationAndAllFlights.clearParameters();
					getReservationAndAllFlights.setInt(1, Rid);
					getReservationAndAllFlights.setInt(2, Rid);
					getReservationAndAllFlightsResult = getReservationAndAllFlights.executeQuery();
				} catch (SQLException e2) {
					e2.printStackTrace();
					System.out.println("SQL getReservationAndAllFlights failed!" + e2.getMessage()+ "\n");
					return "Failed to retrieve reservations\n";

				}

				while (getReservationAndAllFlightsResult.next()) {
					// get info for reservation
					int Rid_current = getReservationAndAllFlightsResult.getInt("Rid");
					int paid = getReservationAndAllFlightsResult.getInt("paid");
					// Get info for flight
					if (Rid_current != Rid_prev) {
						if (paid == 0) {
							sb.append("Reservation " + Rid_current + " paid: false:\n");
						} else {
							sb.append("Reservation " + Rid_current + " paid: true:\n");
						}
					}

					Flight f1 = new Flight();
					f1.fid = getReservationAndAllFlightsResult.getInt("fid");
					f1.dayOfMonth = getReservationAndAllFlightsResult.getInt("day_of_month");
					f1.carrierId = getReservationAndAllFlightsResult.getString("carrier_id");
					f1.flightNum = getReservationAndAllFlightsResult.getString("flight_num");
					f1.originCity = getReservationAndAllFlightsResult.getString("origin_city");
					f1.destCity = getReservationAndAllFlightsResult.getString("dest_city");
					f1.time = getReservationAndAllFlightsResult.getInt("actual_time");
					f1.capacity = getReservationAndAllFlightsResult.getInt("capacity");
					f1.price = getReservationAndAllFlightsResult.getInt("price");
					sb.append(f1.toString() + "\n");
				}

			}
			return sb.toString();

		} catch (SQLException e3) {
			e3.printStackTrace();
			System.out.println("SQL getReservationAndAllFlights failed!" + e3.getMessage()+ "\n");
			return "Failed to retrieve reservations\n";
		}
		finally {
			checkDanglingTransaction();
		}
	}

	/**
	 * Implements the cancel operation.
	 *
	 * @param reservationId the reservation ID to cancel
	 * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n" For
	 * all other errors, return "Failed to cancel reservation [reservationId]\n"
	 * <p>
	 * If successful, return "Canceled reservation [reservationId]\n"
	 * <p>
	 * Even though a reservation has been canceled, its ID should not be reused by the system.
	 */
	public String transaction_cancel(int reservationId) {
		System.out.println(loggedInUserName+" called cancel");
		if (loggedInUserName == null) {
			return "not logged in\n";
		}
		try {
			getReservationById.clearParameters();
			getReservationById.setString(1,loggedInUserName);
			getReservationById.setInt(2,reservationId);
			ResultSet reservation = getReservationById.executeQuery();
			if (reservation.next()) {
				int fid1 = reservation.getInt("fid1");
				int fid2 = reservation.getInt("fid1");
				int canceled = reservation.getInt("canceled");
				int paid = reservation.getInt("paid");
				reservation.close();
				if (canceled == 0) {// if the reservation is not yet canceled
					boolean isDeadLockOuter = false;
					int retriesOuter = 3;
					do {
						try {
							beginTransaction();
							cancelReservation.clearParameters();
							cancelReservation.setInt(1,reservationId);
							cancelReservation.executeUpdate();
							if (paid == 0) { // if the reservation is not yet paid
								commitTransaction();
								return "Canceled reservation " + reservationId + "\n";
							} else { // need to refund
								int price = getPrice(fid1);
								if (fid2 != -1) { // if second flight exists
									price += getPrice(fid2);
								}
								getUserBalance.clearParameters();
								getUserBalance.setString(1,loggedInUserName);
								ResultSet account = getUserBalance.executeQuery();
								account.next();
								int balance = account.getInt("balance");
								balance+=price;
								account.close();
								boolean isDeadLockInner = false;
								int retriesInner = 3;
								do {
									try {
										updateUserBalance.clearParameters();
										updateUserBalance.setInt(1,balance);
										updateUserBalance.setString(2,loggedInUserName);
										updateUserBalance.executeUpdate();
										commitTransaction();
										return "Canceled reservation " + reservationId + "\n";
									} catch (SQLException e1){
										e1.printStackTrace();
										isDeadLockInner = isDeadLock(e1);
										if (!isDeadLockInner) {
											rollbackTransaction();
										}
									}
								}
								while (isDeadLockInner && retriesInner-- > 0);
							}
						} catch (SQLException e){
							e.printStackTrace();
							isDeadLockOuter = isDeadLock(e);
							if (!isDeadLockOuter) {
								rollbackTransaction();
								System.out.println("Cancel reservation failed");
							}
						}
					}
					while (isDeadLockOuter && retriesOuter-- > 0);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		finally {
			checkDanglingTransaction();
		}

		return "Failed to cancel reservation " + reservationId + "\n";
	}

	/**
	 * Example utility function that uses prepared statements
	 */
	private int checkFlightCapacity(int fid) {
		try {
			checkFlightCapacityStatement.clearParameters();
			checkFlightCapacityStatement.setInt(1, fid);
			ResultSet results = checkFlightCapacityStatement.executeQuery();
			if (results.next()) {
				int capacity = results.getInt("capacity");
				results.close();
				return capacity;
			}
		} catch (SQLException e) {
			String msg = e.getMessage();
			throw new IllegalStateException("Check flight capacity failed, " + msg + "!\n");
		}
		throw new IllegalStateException("Cannot get capacity, flight id might not exist!\n");
	}

	/**
	 * Throw IllegalStateException if transaction not completely complete, rollback.
	 */
	private void checkDanglingTransaction() {
		try {
			try (ResultSet rs = tranCountStatement.executeQuery()) {
				rs.next();
				int count = rs.getInt("tran_count");
				if (count > 0) {
					throw new IllegalStateException(
						"Transaction not fully commit/rollback. Number of transaction in process: " + count);
				}
			} finally {
				conn.setAutoCommit(true);
			}
		} catch (SQLException e) {
			throw new IllegalStateException("Database error", e);
		}
	}

	private static boolean isDeadLock(SQLException ex) {
		return ex.getErrorCode() == 1205;
	}
	/* some utility functions below */

	public void beginTransaction() throws SQLException {
		conn.setAutoCommit(false);
		beginTransactionStatement.executeUpdate();
	}

	public void commitTransaction() throws SQLException {
		commitTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}

	public void rollbackTransaction() throws SQLException {
		rollbackTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}

	/**
	 * A class to store flight information.
	 */
	class Flight {
		public int fid;
		public int dayOfMonth;
		public String carrierId;
		public String flightNum;
		public String originCity;
		public String destCity;
		public int time;
		public int capacity;
		public int price;

		@Override
		public String toString() {
			return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId + " Number: "
				+ flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time
				+ " Capacity: " + capacity + " Price: " + price;
		}

	}
}


