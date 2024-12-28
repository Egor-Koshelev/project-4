WITH first_payments AS 
(
SELECT
	user_id,
	min(transaction_datetime::date) AS first_payment_date
FROM
	skyeng_db.payments
WHERE
	status_name = 'success'
GROUP BY
	user_id
ORDER BY
	user_id),

 all_dates AS 
 (
SELECT
	DISTINCT class_start_datetime::date AS dt
FROM
	skyeng_db.classes
WHERE
	class_start_datetime::date BETWEEN '2016-01-01' AND '2016-12-31'),

all_dates_by_user AS 
(
SELECT
	user_id,
	dt
FROM
	all_dates
LEFT JOIN first_payments
ON
	all_dates.dt >= first_payments.first_payment_date),

payments_by_dates AS
(
SELECT
	user_id,
	transaction_datetime::date AS payment_date,
	sum (classes) AS transaction_balance_change
FROM
	skyeng_db.payments
WHERE
	status_name = 'success'
GROUP BY
	user_id,
	payment_date
ORDER BY
	user_id),

payments_by_dates_cumsum AS
(
SELECT
	all_dates_by_user.user_id,
	all_dates_by_user.dt,
	payments_by_dates.transaction_balance_change,
	sum (COALESCE (transaction_balance_change,
	0)) OVER (PARTITION BY all_dates_by_user.user_id
ORDER BY
	all_dates_by_user.dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS transaction_balance_change_cs
FROM
	all_dates_by_user
LEFT JOIN payments_by_dates
ON
	all_dates_by_user.user_id = payments_by_dates.user_id
	AND all_dates_by_user.dt = payments_by_dates.payment_date),

classes_by_dates AS (
SELECT
	user_id,
	class_start_datetime::date AS class_date,
	count(id_class)*-1 AS classes
FROM
	skyeng_db.classes
WHERE
	class_status IN ('success', 'failed_by_student')
		AND class_type != 'trial'
	GROUP BY
		user_id,
		class_date),

classes_by_dates_cumsum AS 
(
SELECT
	all_dates_by_user.user_id,
	dt,
	classes_by_dates.classes,
	sum (COALESCE (classes,
	0)) OVER (PARTITION BY all_dates_by_user.user_id
ORDER BY
	all_dates_by_user.dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS classes_cs
FROM
	all_dates_by_user
LEFT JOIN classes_by_dates
 ON
	all_dates_by_user.user_id = classes_by_dates.user_id
	AND all_dates_by_user.dt = classes_by_dates.class_date),
 
 balances AS (
SELECT
	payments_by_dates_cumsum.user_id,
	payments_by_dates_cumsum.dt,
	payments_by_dates_cumsum.transaction_balance_change,
	payments_by_dates_cumsum.transaction_balance_change_cs,
	classes_by_dates_cumsum.classes,
	classes_by_dates_cumsum.classes_cs,
	(classes_by_dates_cumsum.classes_cs + payments_by_dates_cumsum.transaction_balance_change_cs) AS balance
FROM
	payments_by_dates_cumsum
JOIN classes_by_dates_cumsum
ON
	payments_by_dates_cumsum.user_id = classes_by_dates_cumsum.user_id
	AND payments_by_dates_cumsum.dt = classes_by_dates_cumsum.dt)

SELECT
	*
FROM
	balances
ORDER BY
	balances.user_id,
	balances.dt
LIMIT 1000
