package com.pluralsight.order.dao;

import com.pluralsight.order.dto.OrderDto;
import com.pluralsight.order.dto.OrderDetailDto;
import com.pluralsight.order.util.Database;
import com.pluralsight.order.util.ExceptionHandler;
import com.pluralsight.order.util.OrderStatus;

import java.sql.*;
import java.time.LocalDateTime;

/**
 * DAO to insert an order
 */
public class InsertOrderDao {
    private String sqlOrder = "INSERT INTO orders " +
            "(order_customer_id, order_date, order_status) " +
            "VALUES (?, ?, ?)";
    String sqlOrderDetail =
            "INSERT INTO order_details "
                    + "(order_detail_order_id, order_detail_product_id, order_detail_quantity) "
                    + "VALUES (?, ?, ?)";
    private Database database;

    /**
     * Constructor
     * @param database Database object
     */
    public InsertOrderDao(Database database) {
        this.database = database;
    }

    /**
     * Inserts an order
     * @param orderDto Object with the information to insert
     * @return The ID of the order inserted
     */
    public long insertOrder(OrderDto orderDto) {
        long orderId = -1;

        // tries to get a valid database connection
        try (Connection con = database.getConnection();
             PreparedStatement ps = createOrderPreparedStatement(con, orderDto)
        ) {

            // disables the auto-commit mode on the Connection obj
            con.setAutoCommit(false);
            // executes the insert operation on the PreparedStatement obj
            ps.executeUpdate();

            // Tries to set the ResultSet Obj to the identifier of the inserted order
            try (ResultSet result = ps.getGeneratedKeys()) {
                if(result != null) {
                    if(!result.next()){ // if the ResultSet obj returns false
                        // rolls back the transaction
                        con.rollback();
                    } else { // the ResultSet obj returns true
                        // orderId gets the identifier of the inserted order
                        orderId = result.getLong("order_id");

                        for (OrderDetailDto orderDetailDto : orderDto.getOrderDetail()) {
                            orderDetailDto.setOrderId(orderId);

                            try (PreparedStatement detailsPS =
                                         createOrderDetailPreparedStatement(con, orderDetailDto)) {
                                // executes the insert operation on the PreparedStatement obj
                                int count = detailsPS.executeUpdate();

                                // rollback if insert operation returns a num different other than 1
                                if (count != 1)
                                {
                                    con.rollback();
                                    orderId = -1;
                                }
                            }
                        }
                        // commits the transaction
                        con.commit();
                    }
                }
            } catch(SQLException ex) {
                con.rollback();
                ExceptionHandler.handleException(ex);
            }
        } catch (SQLException ex) {
            ExceptionHandler.handleException(ex);
        }

        return orderId;
    }

    /**
     * Creates a PreparedStatement object to insert the order record
     * @param con Connection object
     * @param orderDto Object with the parameters to set on the PreparedStatement
     * @return A PreparedStatement object
     * @throws SQLException In case of an error
     */
    private PreparedStatement createOrderPreparedStatement(Connection con, OrderDto orderDto) throws SQLException {
        // created a PreparedStatement Obj using the sqlOrder and Statement.Return_Generated_Keys
        PreparedStatement ps = con.prepareStatement(sqlOrder, Statement.RETURN_GENERATED_KEYS);
        // Set the ID of the customer as the 1st param of the query
        ps.setLong(1, orderDto.getCustomerId());
        // Set the current date and time as the 2nd param of the query
        ps.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
        // Set the status of the order as "Created" as the third param of the query
        ps.setString(3, OrderStatus.CREATED.getStatus());
        return ps;
    }

    /**
     * Creates a PreparedStatement object to insert the details of the order
     * @param con Connnection object
     * @param orderDetailDto Object with the parameters to set on the PreparedStatement
     * @return A PreparedStatement object
     * @throws SQLException In case of an error
     */
    private PreparedStatement createOrderDetailPreparedStatement(Connection con, OrderDetailDto orderDetailDto) throws SQLException {
        PreparedStatement ps = con.prepareStatement(sqlOrderDetail);
        ps.setLong(1, orderDetailDto.getOrderId());
        ps.setLong(2, orderDetailDto.getProductId());
        ps.setLong(3, orderDetailDto.getQuantity());
        return ps;
    }
}
