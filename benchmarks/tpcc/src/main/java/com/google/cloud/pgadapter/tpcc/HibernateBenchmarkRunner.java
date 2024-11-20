package com.google.cloud.pgadapter.tpcc;

import com.google.cloud.pgadapter.tpcc.config.PGAdapterConfiguration;
import com.google.cloud.pgadapter.tpcc.config.SpannerConfiguration;
import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import com.google.cloud.pgadapter.tpcc.entities.Customer;
import com.google.cloud.pgadapter.tpcc.entities.CustomerId;
import com.google.cloud.pgadapter.tpcc.entities.District;
import com.google.cloud.pgadapter.tpcc.entities.DistrictId;
import com.google.cloud.pgadapter.tpcc.entities.History;
import com.google.cloud.pgadapter.tpcc.entities.HistoryId;
import com.google.cloud.pgadapter.tpcc.entities.Item;
import com.google.cloud.pgadapter.tpcc.entities.NewOrder;
import com.google.cloud.pgadapter.tpcc.entities.Order;
import com.google.cloud.pgadapter.tpcc.entities.OrderId;
import com.google.cloud.pgadapter.tpcc.entities.OrderLine;
import com.google.cloud.pgadapter.tpcc.entities.OrderLineId;
import com.google.cloud.pgadapter.tpcc.entities.Stock;
import com.google.cloud.pgadapter.tpcc.entities.StockId;
import com.google.cloud.pgadapter.tpcc.entities.Warehouse;
import com.google.cloud.spanner.Dialect;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.JoinType;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.criteria.Subquery;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

public class HibernateBenchmarkRunner extends AbstractBenchmarkRunner {

  private final SessionFactory sessionFactory;
  private final ThreadLocal<Session> sessionThreadLocal = new ThreadLocal<>();
  private final Random random = new Random();

  HibernateBenchmarkRunner(
      Statistics statistics,
      SessionFactory sessionFactory,
      TpccConfiguration tpccConfiguration,
      PGAdapterConfiguration pgAdapterConfiguration,
      SpannerConfiguration spannerConfiguration,
      Metrics metrics,
      Dialect dialect) {
    super(
        statistics,
        tpccConfiguration,
        pgAdapterConfiguration,
        spannerConfiguration,
        metrics,
        dialect);
    this.sessionFactory = sessionFactory;
  }

  @Override
  void setup() throws SQLException, IOException {
    sessionThreadLocal.set(sessionFactory.openSession());
  }

  @Override
  void teardown() throws SQLException {
    Session session = sessionThreadLocal.get();
    if (session != null) {
      session.close();
      sessionThreadLocal.remove();
    }
  }

  @Override
  public void newOrder() throws SQLException {
    Session session = sessionThreadLocal.get();

    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long districtId = Long.reverse(random.nextInt(tpccConfiguration.getDistrictsPerWarehouse()));
    long customerId = Long.reverse(random.nextInt(tpccConfiguration.getCustomersPerDistrict()));

    int orderLineCount = random.nextInt(5, 16);
    long[] itemIds = new long[orderLineCount];
    long[] supplyWarehouses = new long[orderLineCount];
    int[] quantities = new int[orderLineCount];
    int rollback = random.nextInt(100);
    int allLocal = 1;

    for (int line = 0; line < orderLineCount; line++) {
      if (rollback == 1 && line == orderLineCount - 1) {
        itemIds[line] = Long.reverse(Long.MAX_VALUE);
      } else {
        itemIds[line] = Long.reverse(random.nextInt(tpccConfiguration.getItemCount()));
      }
      if (random.nextInt(100) == 50) {
        supplyWarehouses[line] = getOtherWarehouseId(warehouseId);
        allLocal = 0;
      } else {
        supplyWarehouses[line] = warehouseId;
      }
      quantities[line] = random.nextInt(1, 10);
    }

    Transaction tx = session.beginTransaction();
    try {
      Customer customer = session.get(Customer.class, new CustomerId(customerId, districtId, warehouseId));
      District district = customer.getDistrict();
      Warehouse warehouse = district.getWarehouse();

      // Update district next order ID
      long districtNextOrderId = district.getdNextOId();
      district.setdNextOId(districtNextOrderId + 1);

      // Create new Order and NewOrder entities
      Order order = new Order();
      OrderId orderId = new OrderId(districtNextOrderId, customerId, districtId, warehouseId);
      order.setId(orderId);
      order.setEntryD(new Timestamp(System.currentTimeMillis()));
      order.setOlCnt(orderLineCount);
      order.setAllLocal(allLocal);
      order.setCustomer(customer);


      NewOrder newOrder = new NewOrder();
      newOrder.setId(orderId);
      newOrder.setOrder(order);


      // Create and process order lines
      List<OrderLine> orderLines = new ArrayList<>();
      for (int line = 0; line < orderLineCount; line++) {
        long orderLineItemId = itemIds[line];

        Item item = session.get(Item.class, orderLineItemId);
        if (item == null) {
          //item not found, rollback(1% chance)
          tx.rollback();
          return;
        }

        Stock stock = session.get(Stock.class, new StockId(orderLineItemId, supplyWarehouses[line]));
        String[] stockDistrict = {
            stock.getDist01(),
            stock.getDist02(),
            stock.getDist03(),
            stock.getDist04(),
            stock.getDist05(),
            stock.getDist06(),
            stock.getDist07(),
            stock.getDist08(),
            stock.getDist09(),
            stock.getDist10()
        };
        String orderLineDistrictInfo =
            stockDistrict[(int) (Long.reverse(districtId) % stockDistrict.length)];

        long orderLineQuantity = quantities[line];
        long stockQuantity = stock.getQuantity();
        if (stockQuantity > orderLineQuantity) {
          stockQuantity = stockQuantity - orderLineQuantity;
        } else {
          stockQuantity = stockQuantity - orderLineQuantity + 91;
        }
        stock.setQuantity(stockQuantity); // Update stock quantity

        // Calculate order line amount
        BigDecimal totalTax = warehouse.getwTax().add(district.getdTax()).add(BigDecimal.ONE);
        BigDecimal discountFactor = BigDecimal.ONE.subtract(customer.getDiscount());
        BigDecimal orderLineAmount =
            BigDecimal.valueOf(orderLineQuantity)
                .multiply(item.getiPrice())
                .multiply(totalTax)
                .multiply(discountFactor);

        // Create and add order line to the list
        OrderLine orderLine = new OrderLine();
        orderLine.setId(new OrderLineId(districtNextOrderId, customerId, districtId, warehouseId, line));
        orderLine.setOlIId(orderLineItemId);
        orderLine.setOlSupplyWId(supplyWarehouses[line]);
        orderLine.setOlQuantity(orderLineQuantity);
        orderLine.setOlAmount(orderLineAmount);
        orderLine.setOlDistInfo(orderLineDistrictInfo);
        orderLine.setOrder(order); // Set the order for the order line
        orderLines.add(orderLine);
      }

      // Add order lines to the order
      order.setOrderLines(orderLines);
      session.persist(order);
      session.persist(newOrder);

      tx.commit();
    } catch (Exception e) {
      if (tx != null) {
        tx.rollback();
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void payment() throws SQLException {
    Session session = sessionThreadLocal.get();
    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long districtId = Long.reverse(random.nextInt(tpccConfiguration.getDistrictsPerWarehouse()));
    long customerId = Long.reverse(random.nextInt(tpccConfiguration.getCustomersPerDistrict()));
    BigDecimal amount = BigDecimal.valueOf(random.nextInt(1, 5000));

    long customerWarehouseId;
    long customerDistrictId;
    String lastName = LastNameGenerator.generateLastName(this.random, Long.MAX_VALUE);
    boolean byName;

    if (random.nextInt(100) < 60) {
      byName = true;
    } else {
      byName = false;
    }
    if (random.nextInt(100) < 85) {
      customerWarehouseId = warehouseId;
      customerDistrictId = districtId;
    } else {
      customerWarehouseId = getOtherWarehouseId(warehouseId);
      customerDistrictId =
          Long.reverse(random.nextInt(tpccConfiguration.getDistrictsPerWarehouse()));
    }
    Transaction tx = session.beginTransaction();
    try {
      // update warehouse YTD amount
      Warehouse warehouse = session.get(Warehouse.class, warehouseId);
      warehouse.setwYtd(warehouse.getwYtd().add(amount));

      // Update district YTD amount
      District district = session.get(District.class, new DistrictId(districtId, warehouseId));
      district.setdYtd(district.getdYtd().add(amount));

      if (byName) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        // Count customers with the given last name
        CriteriaQuery<Long> countQuery = cb.createQuery(Long.class);
        Root<Customer> countRoot = countQuery.from(Customer.class);
        countQuery.select(cb.count(countRoot));
        countQuery.where(
            cb.equal(countRoot.get("id").get("wId"), customerWarehouseId),
            cb.equal(countRoot.get("id").get("dId"), customerDistrictId),
            cb.equal(countRoot.get("last"), lastName)
        );
        int nameCount = session.createQuery(countQuery).getSingleResult().intValue();
        if (nameCount % 2 == 0) {
          nameCount++;
        }

        // Retrieve customer ID (using nameCount as a pseudo-random offset)
        CriteriaQuery<Long> idQuery = cb.createQuery(Long.class);
        Root<Customer> idRoot = idQuery.from(Customer.class);
        idQuery.select(idRoot.get("id").get("cId"));
        idQuery.where(
            cb.equal(idRoot.get("id").get("wId"), customerWarehouseId),
            cb.equal(idRoot.get("id").get("dId"), customerDistrictId),
            cb.equal(idRoot.get("last"), lastName)
        );
        idQuery.orderBy(cb.asc(idRoot.get("first")));

        List<Long> results = session.createQuery(idQuery).getResultList();
        for (int counter = 0; counter < Math.min(nameCount, results.size()); counter++) {
          customerId = results.get(counter);
        }
      }

      // Update customer
      Customer customer = session.get(Customer.class, new CustomerId(customerId, customerDistrictId, customerWarehouseId));
      if (customer != null) {
        customer.setBalance(customer.getBalance().subtract(amount));
        customer.setYtdPayment(customer.getYtdPayment().add(amount));

        if ("BC".equals(customer.getCredit())) {
          String customerData = customer.getData();
          String newCustomerData = String.format(
              "| %4d %2d %4d %2d %4d $%7.2f %12s %24s",
              customerId,
              customerDistrictId,
              customerWarehouseId,
              districtId,
              warehouseId,
              amount,
              LocalDateTime.now(),
              customerData);
          if (newCustomerData.length() > 500) {
            newCustomerData = newCustomerData.substring(0, 500);
          }
          customer.setData(newCustomerData);
        }

        // Insert history
        History history = new History();
        history.setId(new HistoryId(customerId, customerDistrictId, customerWarehouseId, districtId, warehouseId, new Timestamp(System.currentTimeMillis())));
        history.setAmount(amount);
        history.setData(String.format("%10s %10s", warehouse.getwName(), district.getdName()));
        session.persist(history);
      }

      tx.commit();
    } catch (Exception e) {
      if (tx != null) {
        tx.rollback();
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void orderStatus() throws SQLException {
    Session session = sessionThreadLocal.get();

    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long districtId = Long.reverse(random.nextInt(tpccConfiguration.getDistrictsPerWarehouse()));
    long customerId = Long.reverse(random.nextInt(tpccConfiguration.getCustomersPerDistrict()));

    String lastName = LastNameGenerator.generateLastName(this.random, Long.MAX_VALUE);
    boolean byName = random.nextInt(100) < 60;

    Transaction tx = session.beginTransaction();
    try {
      Customer customer = null;
      if (byName) {
        CriteriaBuilder cb = session.getCriteriaBuilder();

        // Count customers with the given last name
        CriteriaQuery<Long> countQuery = cb.createQuery(Long.class);
        Root<Customer> countRoot = countQuery.from(Customer.class);
        countQuery.select(cb.count(countRoot));

        countQuery.where(
            cb.equal(countRoot.get("id").get("wId"), warehouseId),
            cb.equal(countRoot.get("id").get("dId"), districtId),
            cb.equal(countRoot.get("last"), lastName)
        );
        int nameCount = session.createQuery(countQuery).getSingleResult().intValue();

        if (nameCount % 2 == 0) {
          nameCount++;
        }

        // retrieve customer
        CriteriaQuery<Customer> customerQuery = cb.createQuery(Customer.class);
        Root<Customer> idRoot = customerQuery.from(Customer.class);
        customerQuery.where(
            cb.equal(idRoot.get("id").get("wId"), warehouseId),
            cb.equal(idRoot.get("id").get("dId"), districtId),
            cb.equal(idRoot.get("last"), lastName)
        );
        customerQuery.orderBy(cb.asc(idRoot.get("first")));

        List<Customer> results = session.createQuery(customerQuery).getResultList();
        for (int counter = 0; counter < Math.min(nameCount, results.size()); counter++) {
          customer = results.get(counter);
          customerId = customer.getId().getcId();
        }
      } else {
        customer = session.get(Customer.class, new CustomerId(customerId, warehouseId, districtId));
      }

      if (customer != null) {
        // Fetch the latest order for the customer
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<Order> orderQuery = cb.createQuery(Order.class);
        Root<Order> orderRoot = orderQuery.from(Order.class);
        orderQuery.where(
            cb.equal(orderRoot.get("id").get("wId"), warehouseId),
            cb.equal(orderRoot.get("id").get("dId"), districtId),
            cb.equal(orderRoot.get("id").get("cId"), customerId)
        );
        orderQuery.orderBy(cb.desc(orderRoot.get("id").get("oId")));

        Order order = session.createQuery(orderQuery)
            .setMaxResults(1)
            .getSingleResult();

        List<OrderLine> orderLines = order.getOrderLines();
        for (OrderLine orderLine : orderLines) {
          Long id = orderLine.getOlIId();
        }
      }
      tx.commit();
    } catch (Exception e) {
      if (tx != null) {
        tx.rollback();
      }
      throw new RuntimeException(e);
    }
  }


  @Override
  public void delivery() throws SQLException {
    Session session = sessionThreadLocal.get();
    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long carrierId = Long.reverse(random.nextInt(10));
    Transaction tx = session.beginTransaction();
    try {
      for (long district = 0L; district < tpccConfiguration.getDistrictsPerWarehouse(); district++) {
        // find the oldest new_order in district
        long districtId = Long.reverse(district);
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<NewOrder> query = cb.createQuery(NewOrder.class);
        Root<NewOrder> root = query.from(NewOrder.class);
        query.where(
            cb.equal(root.get("id").get("dId"), districtId),
            cb.equal(root.get("id").get("wId"), warehouseId)
        );
        query.orderBy(cb.asc(root.get("id").get("oId")));
        NewOrder newOrder = session.createQuery(query)
            .setMaxResults(1)
            .getSingleResult();


        if (newOrder != null) {
          // Get the Order and Customer entities
          Order order = newOrder.getOrder();
          Customer customer = order.getCustomer();
          // mark newOrder for removal
          session.remove(newOrder);

          // Update the corresponding order with the carrier ID
          order.setCarrierId(carrierId);

          // Update the delivery date in the order lines
          for (OrderLine orderLine : order.getOrderLines()) {
            Timestamp t = new Timestamp(System.currentTimeMillis());
            orderLine.setOlDeliveryD(t);
          }

          // Calculate the sum of order line amounts
          BigDecimal sumOrderLineAmount = order.getOrderLines().stream()
              .map(OrderLine::getOlAmount)
              .reduce(BigDecimal.ZERO, BigDecimal::add);

          // Update the customer's balance and delivery count
          customer.setBalance(customer.getBalance().add(sumOrderLineAmount));
          customer.setDeliveryCnt(customer.getDeliveryCnt() + 1);
        }
      }
      tx.commit();
    } catch (Exception e) {
      if (tx != null) {
        tx.rollback();
      }
      throw new RuntimeException(e);
    }
  }

  public void stockLevel() throws SQLException {
    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long districtId = Long.reverse(random.nextInt(tpccConfiguration.getDistrictsPerWarehouse()));
    int level = random.nextInt(10, 21);

    Session session = sessionThreadLocal.get();
    Transaction tx = session.beginTransaction();

    try {
      CriteriaBuilder cb = session.getCriteriaBuilder();

      // Retrieve the next available order number for the district
      CriteriaQuery<Long> nextOrderQuery = cb.createQuery(Long.class);
      Root<District> districtRoot = nextOrderQuery.from(District.class);
      nextOrderQuery.select(districtRoot.get("dNextOId"));
      nextOrderQuery.where(
          cb.equal(districtRoot.get("id").get("dId"), districtId),
          cb.equal(districtRoot.get("id").get("wId"), warehouseId)
      );

      long nextOrderId = session.createQuery(nextOrderQuery).getSingleResult();

      CriteriaQuery<Long> query = cb.createQuery(Long.class);
      Root<OrderLine> orderLine = query.from(OrderLine.class);
      Root<Stock> stock = query.from(Stock.class);

      query.select(cb.countDistinct(stock.get("id").get("sIId")));

      query.where(
          cb.equal(orderLine.get("id").get("wId"), warehouseId),
          cb.equal(orderLine.get("id").get("dId"), districtId),
          cb.lt(orderLine.get("id").get("oId"), nextOrderId),
          cb.greaterThanOrEqualTo(orderLine.get("id").get("oId"), nextOrderId - 20),
          cb.equal(stock.get("id").get("wId"), warehouseId),
          cb.equal(orderLine.get("olIId"), stock.get("id").get("sIId")),
          cb.lt(stock.get("quantity"), level)
      );
      List<Long> result = session.createQuery(query).getResultList();

      // Iterate through the order lines and count stock items with quantity below threshold
      for (Long orderLineItemId : result) {
        CriteriaQuery<Long> stockQuery = cb.createQuery(Long.class);
        Root<Stock> stockRoot = stockQuery.from(Stock.class);
        stockQuery.select(cb.count(stockRoot));
        stockQuery.where(
            cb.equal(stockRoot.get("id").get("wId"), warehouseId),
            cb.equal(stockRoot.get("id").get("sIId"), orderLineItemId),
            cb.lt(stockRoot.get("quantity"), level)
        );
        long stockCount = session.createQuery(stockQuery).getSingleResult();
      }

      tx.commit();
    } catch (Exception e) {
      if (tx != null) {
        tx.rollback();
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  void executeStatement(String sql) throws SQLException {}

  @Override
  Object[] paramQueryRow(String sql, Object[] params) throws SQLException {
    return new Object[0];
  }

  @Override
  void executeParamStatement(String sql, Object[] params) throws SQLException {}

  @Override
  List<Object[]> executeParamQuery(String sql, Object[] params) throws SQLException {
    return null;
  }
}
