/*
 * Copyright (C) 2016 - 2017 Xplanr Analytics Inc, All rights reserved. 
 *
 * File Name	: UDFRegister.java
 * 
 * Created By	: 
 * 
 * Created Date	: 24-May-2017
 */
package com.platformx.framework.udf;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.commons.math3.util.Precision;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.analysis.function.Log;
import org.apache.poi.ss.formula.functions.FinanceLib;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.api.java.UDF7;
import org.apache.spark.sql.api.java.UDF8;
import org.apache.spark.sql.types.DataTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class UDFRegister implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final static Logger logger = LoggerFactory.getLogger(UDFRegister.class);

	public UDFRegister(SQLContext sqlContext) {

		// Register NORM.S.INV
		sqlContext.udf().register("normSInv", new UDF1<Double, Double>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double p) throws Exception {

				NormalDistribution nd = new NormalDistribution();
				return nd.inverseCumulativeProbability(p);
			}
		}, DataTypes.DoubleType);

		// Register NORM.S.DIST
		sqlContext.udf().register("normSDist", new UDF1<Double, Double>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double p) throws Exception {

				NormalDistribution nd = new NormalDistribution();
				return nd.cumulativeProbability(p);
			}
		}, DataTypes.DoubleType);
		
		// Register NORM.DIST
		sqlContext.udf().register("normDist", new UDF3<Double, Double, Double, Double>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double x, Double mean, Double std_dev) throws Exception {

				NormalDistribution nd = new NormalDistribution(mean, std_dev);
				return nd.cumulativeProbability(x);
			}
		}, DataTypes.DoubleType);
				
		// Register BETADIST
		sqlContext.udf().register("betaDist", new UDF3<Double, Double, Double, Double>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double x, Double alpha, Double beta) throws Exception {

				BetaDistribution bd = new BetaDistribution(alpha, beta);
				return bd.cumulativeProbability(x);
			}
		}, DataTypes.DoubleType);

		// Register MAX
		sqlContext.udf().register("maxOf", new UDF2<Double, Double, Double>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double value1, Double value2) throws Exception {

				double[] maxArray = new double[2];
				maxArray[0] = value1;
				maxArray[1] = value2;
				return NumberUtils.max(maxArray);
			}
		}, DataTypes.DoubleType);

		// Register MAX
		sqlContext.udf().register("minOf", new UDF2<Double, Double, Double>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double value1, Double value2) throws Exception {

				double[] maxArray = new double[2];
				// BigDecimal b;
				maxArray[0] = value1.doubleValue();
				maxArray[1] = value2.doubleValue();
				return NumberUtils.min(maxArray);
				// return b = new BigDecimal(NumberUtils.min(maxArray),
				// MathContext.DECIMAL64);
			}
		}, DataTypes.DoubleType);

		// IIR Calculation
		sqlContext.udf().register("irrCalc", new UDF5<Double, Double, Double, Double, WrappedArray<Double>, Double>() {

			/** */
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double principalamount, Double fee, Double initialirr, Double errorThreshold,
					WrappedArray<Double> wCashflow) throws Exception {

				Double errvalue;
				Object b[] = new Object[wCashflow.length()];
				wCashflow.copyToArray(b);

				double[] cashflow = new double[wCashflow.length()];
				int index = 0;
				for (int i = 0; i < b.length; i++) {
					Object object = b[i];
					String string = object.toString();
					double dub = Double.valueOf(string).doubleValue();
					System.out.println(dub);
					cashflow[index] = dub;
					++index;
				}
				// HardCoding because the fee is stored in
				fee = -fee;
				Double total_cashflow = Double.valueOf(fee);
				for (int i = 0; i < cashflow.length; i++) {
					total_cashflow += cashflow[i];
				}
				total_cashflow = -total_cashflow;
				double npv = 0;
				while (true) {
					npv = FinanceLib.npv(initialirr / 12, cashflow) + fee;
					errvalue = principalamount + npv;
					double value1 = principalamount / total_cashflow;
					double value2 = -npv / total_cashflow;
					initialirr = initialirr * Math.log(value1) / Math.log(value2);

					if (!Double.isNaN(errvalue) && errvalue >= errorThreshold) {
						break;
					}
				}

				return initialirr;
			}
		}, DataTypes.DoubleType);

		// EIR Calculation
		sqlContext.udf().register("eirV11",
				new UDF6<Double, Double, Double, WrappedArray<Double>, WrappedArray<String>, Double, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double loan_amt, Double fee_amt, Double seed, WrappedArray<Double> wCashflow,
							WrappedArray<String> wCashflowDate, Double target) throws Exception {

						Object b[] = new Object[wCashflow.length()];
						wCashflow.copyToArray(b);
						Object d[] = new Object[wCashflowDate.length()];
						wCashflowDate.copyToArray(d);

						double[] cashflow = new double[wCashflow.length()];
						Date[] dates = new Date[wCashflow.length()];
						double[] opening = new double[wCashflow.length()];
						double[] closing = new double[wCashflow.length()];
						double[] interest = new double[wCashflow.length()];
						double[] bal_time = new double[wCashflow.length()];
						double sum_int_eir = 0.0d;
						double sum_bal_time = 0.0d;
						double r = seed;
						int index = 0;

						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							cashflow[index] = dub;

							object = d[i];
							string = object.toString();
							dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
							++index;
						}

						index = 0;
						while (true) {

							for (int i = 0; i < b.length; i++) {
								if (i == 0) {
									opening[i] = loan_amt - fee_amt;
									interest[i] = 0.0d;
									bal_time[i] = 0.0d;
									closing[i] = opening[i] - cashflow[i];
								} else {
									double duration = TimeUnit.MILLISECONDS
											.toDays(dates[i].getTime() - dates[i - 1].getTime());
									opening[i] = closing[i - 1];
									interest[i] = opening[i] * (duration / 365) * r;
									bal_time[i] = opening[i] * (duration / 365);
									closing[i] = opening[i] + interest[i] - cashflow[i];
								}
							}

							sum_int_eir = 0.0d;
							sum_bal_time = 0.0d;
							for (int i = 0; i < cashflow.length; i++) {
								sum_int_eir += interest[i];
								sum_bal_time += bal_time[i];
							}

							if (index > 199) {
								logger.debug("Programs gonna break now due to too many iterations: " + r);
								break;
							}
							if (Double.isNaN(r)) {
								logger.debug("Programs gonna break now due to NaN : " + r);
								break;
							}
							if (Math.abs(sum_int_eir - target) < 0.000001) {
								logger.debug("Programs gonna break now due to r : " + r);
								break;
							}

							r = r - (sum_int_eir - target) / (sum_bal_time);
							index++;
						}

						return r;
					}
				}, DataTypes.DoubleType);

		// EIR Calculation
		sqlContext.udf().register("eirV13",
				new UDF8<Double, Double, Double, Double, String, WrappedArray<Double>, WrappedArray<String>, Double, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double loan_amt, Double fee_amt, Double seed, Double mod_amt, String mod_date,
							WrappedArray<Double> wCashflow, WrappedArray<String> wCashflowDate, Double target)
							throws Exception {

						Object b[] = new Object[wCashflow.length()];
						wCashflow.copyToArray(b);
						Object d[] = new Object[wCashflowDate.length()];
						wCashflowDate.copyToArray(d);

						double[] cashflow = new double[wCashflow.length()];
						Date[] dates = new Date[wCashflow.length()];
						double[] opening = new double[wCashflow.length()];
						double[] closing = new double[wCashflow.length()];
						double[] interest = new double[wCashflow.length()];
						double[] bal_time = new double[wCashflow.length()];
						double sum_int_eir = 0.0d;
						double sum_bal_time = 0.0d;
						double r = seed;
						int index = 0;
						Date dMod = new SimpleDateFormat("yyyy-MM-dd").parse(mod_date);

						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							cashflow[index] = dub;

							object = d[i];
							string = object.toString();
							dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
							++index;
						}

						index = 0;
						while (true) {

							for (int i = 0; i < b.length; i++) {
								if (i == 0) {
									opening[i] = loan_amt - fee_amt;
									interest[i] = 0.0d;
									bal_time[i] = 0.0d;
									closing[i] = opening[i] - cashflow[i];
								} else {
									double duration = TimeUnit.MILLISECONDS
											.toDays(dates[i].getTime() - dates[i - 1].getTime());
									if (dMod == dates[i]) {
										opening[i] = closing[i - 1] - mod_amt;
									} else {
										opening[i] = closing[i - 1];
									}
									interest[i] = opening[i] * (duration / 365) * r;
									bal_time[i] = opening[i] * (duration / 365);
									closing[i] = opening[i] + interest[i] - cashflow[i];
								}
							}

							sum_int_eir = 0.0d;
							sum_bal_time = 0.0d;
							for (int i = 0; i < cashflow.length; i++) {
								sum_int_eir += interest[i];
								sum_bal_time += bal_time[i];
							}

							if (index > 199) {
								logger.debug("Programs gonna break now due to too many iterations: " + r);
								break;
							}
							if (Double.isNaN(r)) {
								logger.debug("Programs gonna break now due to NaN : " + r);
								break;
							}
							if (Math.abs(sum_int_eir - target) < 0.000001) {
								logger.debug("Programs gonna break now due to r : " + r);
								break;
							}

							r = r - (sum_int_eir - target) / (sum_bal_time);
							index++;
						}

						return r;
					}
				}, DataTypes.DoubleType);

		// Fee Amortization
		sqlContext.udf().register("feeAmort",
				new UDF6<Double, Double, Double, WrappedArray<Double>, WrappedArray<Double>, WrappedArray<String>, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double loan_amt, Double fee_amt, Double eir, WrappedArray<Double> wCashflow,
							WrappedArray<Double> cInterest, WrappedArray<String> wCashflowDate) throws Exception {

						Object b[] = new Object[wCashflow.length()];
						wCashflow.copyToArray(b);
						Object c[] = new Object[cInterest.length()];
						cInterest.copyToArray(c);
						Object d[] = new Object[wCashflowDate.length()];
						wCashflowDate.copyToArray(d);

						double[] cashflow = new double[wCashflow.length()];
						Date[] dates = new Date[wCashflow.length()];
						double[] interest = new double[wCashflow.length()];
						double[] opening = new double[wCashflow.length()];
						double[] closing = new double[wCashflow.length()];
						double[] interestEir = new double[wCashflow.length()];
						double amortFee = 0.0d;
						int index = 0;

						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							cashflow[index] = dub;

							object = c[i];
							string = object.toString();
							interest[index] = Double.valueOf(string).doubleValue();

							object = d[i];
							string = object.toString();
							dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
							++index;
						}

						for (int i = 0; i < b.length; i++) {
							if (i == 0) {
								opening[i] = loan_amt - fee_amt;
								interestEir[i] = 0.0d;
								closing[i] = opening[i] - cashflow[i];
							} else {
								double duration = TimeUnit.MILLISECONDS
										.toDays(dates[i].getTime() - dates[i - 1].getTime());
								opening[i] = closing[i - 1];
								interestEir[i] = opening[i] * (duration / 365) * eir;
								closing[i] = opening[i] + interestEir[i] - cashflow[i];
								amortFee = interestEir[i] - interest[i];
							}
						}

						return amortFee;
					}
				}, DataTypes.DoubleType);

		// XIRR
		sqlContext.udf().register("xirr", new UDF3<Double, WrappedArray<Double>, WrappedArray<String>, Double>() {

			/** */
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double seed, WrappedArray<Double> wCashflow, WrappedArray<String> wCashflowDate)
					throws Exception {

				double r = seed;
				int index = 0;
				int n = wCashflow.length();
				double[] cashflow = new double[n];
				double[] pv_cashflows = new double[n];
				double[] grad = new double[n];
				Date[] dates = new Date[n];
				double sum_pv_cflow = 0.0d;
				double sum_grad = 0.0d;

				Object b[] = new Object[wCashflow.length()];
				wCashflow.copyToArray(b);
				Object c[] = new Object[wCashflowDate.length()];
				wCashflowDate.copyToArray(c);

				for (int i = 0; i < b.length; i++) {
					Object object = b[i];
					String string = object.toString();
					double dub = Double.valueOf(string).doubleValue();
					cashflow[index] = dub;

					object = c[i];
					string = object.toString();
					dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
					++index;
				}

				while (true) {

					sum_pv_cflow = 0.0d;
					sum_grad = 0.0d;
					for (int i = 0; i < n; i++) {
						double duration = TimeUnit.MILLISECONDS.toDays(dates[i].getTime() - dates[1].getTime());
						pv_cashflows[i] = cashflow[i] / Math.pow((1 + r), (duration / 365));
						grad[i] = (-1 * pv_cashflows[i] * (duration / 365)) / (1 + r);
						sum_pv_cflow = sum_pv_cflow + pv_cashflows[i];
						sum_grad = sum_grad + grad[i];
					}

					if (Math.abs(sum_pv_cflow) < 0.000001 || !Double.isNaN(sum_grad)) {
						break;
					}
					r = r - (sum_pv_cflow) / (sum_grad);
				}
				return r;
			}
		}, DataTypes.DoubleType);

		// IRR
		sqlContext.udf().register("irr", new UDF2<Double, WrappedArray<Double>, Double>() {

			/** */
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double seed, WrappedArray<Double> wCashflow) throws Exception {
				double r = seed;
				int index = 0;
				int n = wCashflow.length();
				double[] cashflow = new double[n];
				double[] pv_cashflows = new double[n];
				double[] grad = new double[n];
				double sum_pv_cflow = 0.0d;
				double sum_grad = 0.0d;

				Object b[] = new Object[wCashflow.length()];
				wCashflow.copyToArray(b);

				for (int i = 0; i < b.length; i++) {
					Object object = b[i];
					String string = object.toString();
					double dub = Double.valueOf(string).doubleValue();
					cashflow[index] = dub;
					++index;
				}

				while (true) {
					sum_pv_cflow = 0.0d;
					sum_grad = 0.0d;
					for (int i = 0; i < n; i++) {
						pv_cashflows[i] = cashflow[i] / Math.pow((1 + r / 12), (i - 1));
						grad[i] = (-1 * pv_cashflows[i] * (i - 1)) / (1 + r / 12);
						sum_pv_cflow = sum_pv_cflow + pv_cashflows[i];
						sum_grad = sum_grad + grad[i];
					}

					if (Math.abs(sum_pv_cflow) < 0.000001 || !Double.isNaN(sum_grad)) {
						break;
					}
					r = r - (sum_pv_cflow) / (sum_grad);
					
				}
				return r;
				
			}
		}, DataTypes.DoubleType);

		// for trim
		sqlContext.udf().register("trim", new UDF1<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String value1) throws Exception {

				return value1 == null ? "" : value1.trim();

			}
		}, DataTypes.StringType);
		
		// Extrapolated LGD Calculation (HDFC)
				sqlContext.udf().register("eLgdCalc", new UDF1<WrappedArray<String>, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wRecoveryRate) throws Exception {

						String b[] = new String[wRecoveryRate.length()];
						wRecoveryRate.copyToArray(b);

						String[] predYear = new String[b.length];
						int[] revRecPeriod = new int[b.length];
						Double[] recRate = new Double[b.length];
						Double[] revDefaultOs = new Double[b.length];
						Double[] defaultOs = new Double[b.length];
						
						Double[] extrapolatedRecRate = new Double[b.length];
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							predYear[index] = splitArray[0];
							revRecPeriod[index] = Integer.parseInt(splitArray[1]);
							recRate[index] = Double.valueOf(splitArray[2]).doubleValue();
							revDefaultOs[index] = Double.valueOf(splitArray[3]).doubleValue();
							defaultOs[index] = Double.valueOf(splitArray[4]).doubleValue();
							
							++index;
						}
						
						int currentRevRecPeriod = revRecPeriod[index-1];
						
						for(int i = 0; i < predYear.length - 1 ; i++) {
							int currentRevRecPeriodInLoop = revRecPeriod[i];
							
						  if(defaultOs[i] != 0.0) {
							  extrapolatedRecRate[i] = recRate[i];
						  } 
						  else {
							Double sumProd1InLoop = 0.0;
							Double sumProd2InLoop = 0.0;
								
							for(int j = 0; j < i-1 ; j++) {
								if(revRecPeriod[j] == currentRevRecPeriodInLoop) {
									sumProd1InLoop = sumProd1InLoop + (extrapolatedRecRate[j] * revDefaultOs[j]);
								}
								else if(revRecPeriod[j] == currentRevRecPeriodInLoop - 1) {
									sumProd2InLoop = sumProd2InLoop + (extrapolatedRecRate[j] * revDefaultOs[j]);
								}								
							}
							
								extrapolatedRecRate[i] = sumProd1InLoop / sumProd2InLoop * extrapolatedRecRate[i-1];
						  }
						}						
						
						Double sumProd1 = 0.0;
						Double sumProd2 = 0.0;
						
						for(int k = 0; k < predYear.length - 2 ; k++) {
							if(revRecPeriod[k] == currentRevRecPeriod) {
								sumProd1 = sumProd1 + (extrapolatedRecRate[k] * revDefaultOs[k]);
							}
							else if(revRecPeriod[k] == currentRevRecPeriod - 1) {
								sumProd2 = sumProd2 + (extrapolatedRecRate[k] * revDefaultOs[k]);
							}								
						}
						
						Double result;
						
						if(defaultOs[index-1] == 0.0) {
							result = sumProd1 / sumProd2 * extrapolatedRecRate[index-2];
						}
						else{
							result = recRate[index-1];
						}
								
						return result;
					}
				}, DataTypes.DoubleType);
				
				// PD Lifetime Interim4 Calculation (HDFC)
				sqlContext.udf().register("pdInterim4", new UDF2<WrappedArray<String>, WrappedArray<Double>, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wPreceding, WrappedArray<Double> wAllRows ) throws Exception {
						
						Double a[] = new Double[wAllRows.length()];
						wAllRows.copyToArray(a);
						
						String b[] = new String[wPreceding.length()];
						wPreceding.copyToArray(b);
							
						int[] year = new int[b.length];
						int[] bucket = new int[b.length];
						Double[] pdInterim3 = new Double[b.length];
						
						Double[] pdInterim4 = new Double[b.length];
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							year[index] = Integer.parseInt(splitArray[0]);
							bucket[index] = Integer.parseInt(splitArray[1]);
							pdInterim3[index] = Double.valueOf(splitArray[2]).doubleValue();
							
							++index;
						}
						
						
						for(int i=0; i < year.length; i++) {
								if(year[i] == 1) {
									pdInterim4[i] = pdInterim3[i];
								}
								else if(year[i] == 2) {
									if(a[a.length-1] > a[a.length-2] && a[a.length-2] > a[a.length-3] && 
											a[a.length-3] > a[a.length-4] && a[a.length-4] > a[a.length-5]) {
										pdInterim4[i] = pdInterim3[i];
									}
									else if(pdInterim3[i] < a[i+5]) {
										pdInterim4[i] = pdInterim3[i];
									}
									else {
										pdInterim4[i] = 1- ((1 - pdInterim4[i-5]) * (1 - pdInterim4[i-5]));
									}
								}
								else {
									if(pdInterim3[i-5] == pdInterim4[i-5]) {
										pdInterim4[i] = pdInterim3[i];
									}
									else {
										pdInterim4[i] = 1- ((1 - pdInterim4[i-5]) * (1 - (pdInterim4[i-5] - pdInterim4[i-10])));
									}
								}
						}
						
						Double result = pdInterim4[index-1];
						
						return result;
					}
				}, DataTypes.DoubleType);
				
				// PD Lifetime Interim3 Calculation (HDFC)
				sqlContext.udf().register("pdInterim3", new UDF2<WrappedArray<String>, WrappedArray<Double>, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wPreceding, WrappedArray<Double> wAllRows ) throws Exception {
						
						Double a[] = new Double[wAllRows.length()];
						wAllRows.copyToArray(a);
						
						String b[] = new String[wPreceding.length()];
						wPreceding.copyToArray(b);
							
						int[] year = new int[b.length];
						int[] bucket = new int[b.length];
						Double[] pdInterim2 = new Double[b.length];
						
						Double[] pdInterim3 = new Double[b.length];
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							year[index] = Integer.parseInt(splitArray[0]);
							bucket[index] = Integer.parseInt(splitArray[1]);
							pdInterim2[index] = Double.valueOf(splitArray[2]).doubleValue();
							
							++index;
						}
						
						
						for(int i=0; i < year.length; i++) {
								if(year[i] == 1 || year[i] == 2) {
									pdInterim3[i] = pdInterim2[i];
								}
								else if(year[i] == 3) {
									if(a[a.length-1] > a[a.length-2] && a[a.length-2] > a[a.length-3] && 
											a[a.length-3] > a[a.length-4] && a[a.length-4] > a[a.length-5]) {
										pdInterim3[i] = pdInterim2[i];
									}
									else { 
										if(bucket[i] == 0) {
											pdInterim3[i] = pdInterim2[i];
										}
										else {
											pdInterim3[i] = 1- ((1 - pdInterim3[i-5]) * (1 - (pdInterim3[i-5] - pdInterim3[i-10])));
										}
									}
								}
								else {
									if(pdInterim2[i-5] == pdInterim3[i-5]) {
										pdInterim3[i] = pdInterim2[i];
									}
									else {
										pdInterim3[i] = 1- ((1 - pdInterim3[i-5]) * (1 - (pdInterim3[i-5] - pdInterim3[i-10])));
									}
								}
						}
						
						Double result = pdInterim3[index-1];
						
						return result;
					}
				}, DataTypes.DoubleType);
				
				// PD Lifetime Interim2 Calculation (HDFC)
				sqlContext.udf().register("pdInterim2", new UDF2<WrappedArray<String>, WrappedArray<Double>, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wPreceding, WrappedArray<Double> wAllRows ) throws Exception {
						
						Double a[] = new Double[wAllRows.length()];
						wAllRows.copyToArray(a);
						
						String b[] = new String[wPreceding.length()];
						wPreceding.copyToArray(b);
							
						int[] year = new int[b.length];
						int[] bucket = new int[b.length];
						Double[] pdInterim1 = new Double[b.length];
						
						Double[] pdInterim2 = new Double[b.length];
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							year[index] = Integer.parseInt(splitArray[0]);
							bucket[index] = Integer.parseInt(splitArray[1]);
							pdInterim1[index] = Double.valueOf(splitArray[2]).doubleValue();
							
							++index;
						}
						
						
						for(int i=0; i < year.length; i++) {
								if(year[i] == 1 || year[i] == 2) {
									pdInterim2[i] = pdInterim1[i];
								}
								else {
									if(pdInterim1[i] <= pdInterim1[i-5] || (bucket[i] > 0 && pdInterim1[i] <= pdInterim1[i-1]) ||
										(bucket[i] < 4 && pdInterim1[i] >= a[i+1])	) {
										pdInterim2[i] = 1 - ((1 - pdInterim2[i-5]) * (1 - (pdInterim2[i-5] - pdInterim2[i-10])));
									}
									else {
										pdInterim2[i] = pdInterim1[i];
									}
								}
						}
											
						
						Double result = pdInterim2[index-1];
						
						return result;
					}
				}, DataTypes.DoubleType);
				
				// PD Lifetime Interim1 Calculation (HDFC)
				sqlContext.udf().register("pdInterim1", new UDF2<WrappedArray<String>, WrappedArray<Double>, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wPreceding, WrappedArray<Double> wAllRows ) throws Exception {
						
						Double a[] = new Double[wAllRows.length()];
						wAllRows.copyToArray(a);
						
						String b[] = new String[wPreceding.length()];
						wPreceding.copyToArray(b);
						
						
						int[] year = new int[b.length];
						int[] bucket = new int[b.length];
						Double[] defaultRate = new Double[b.length];
						
						Double[] pdInterim1 = new Double[b.length];
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							year[index] = Integer.parseInt(splitArray[0]);
							bucket[index] = Integer.parseInt(splitArray[1]);
							defaultRate[index] = Double.valueOf(splitArray[2]).doubleValue();
							
							++index;
						}
						
						pdInterim1[0] = defaultRate[0];
						
						for(int i=1; i < year.length; i++) {
							if(defaultRate[i] > 0) {
								pdInterim1[i] = defaultRate[i];
							}
							else {
								if(year[i] == 1 || year[i] == 2) {
									pdInterim1[i] = (defaultRate[i-1] + a[i+1])/2;
								}
								else {
									pdInterim1[i] = 1 - ((1 - pdInterim1[i-5]) * (1 - (pdInterim1[i-5] - pdInterim1[i-10])));
								}
							}
						}
											
						
						Double result = pdInterim1[index-1];
						
						return result;
					}
				}, DataTypes.DoubleType);
				
				// Fair Value Opening Liability Calculation (SIB)
				sqlContext.udf().register("openingLiab", new UDF2<WrappedArray<String>, Double, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wPreceding, Double pvEmi ) throws Exception {
						
						String b[] = new String[wPreceding.length()];
						wPreceding.copyToArray(b);
							
						Double[] rate = new Double[b.length];
						Double[] rateQ2 = new Double[b.length];
						String[] year = new String[b.length];
						int[] cfMonth = new int[b.length];
						Double[] emi = new Double[b.length];
						Double[] openingLiab = new Double[b.length];
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							rate[index] = Double.valueOf(splitArray[0]).doubleValue();
							rateQ2[index] = Double.valueOf(splitArray[1]).doubleValue();
							year[index] = (splitArray[2]).toString();
							cfMonth[index] = Integer.parseInt(splitArray[3]);
							emi[index] = Double.valueOf(splitArray[4]).doubleValue();
							
							++index;
						}
						
						
						for(int i=0; i < cfMonth.length; i++) {
								if(cfMonth[i] == 0) {
									openingLiab[i] = 0.0;
								}
								else {
									if(cfMonth[i] == 1) {
										openingLiab[i] = pvEmi;
									}
									else {
										if(year[i] == "Others") {
											openingLiab[i] = openingLiab[i-1] + (openingLiab[i-1] * rateQ2[i-1] / 12) - emi[i-1];
										}
										else {
											openingLiab[i] = openingLiab[i-1] + (openingLiab[i-1] * rate[i-1] / 12) - emi[i-1];
										}
									}
								}
						}
											
						
						Double result = openingLiab[index-1];
						
						return result;
					}
				}, DataTypes.DoubleType);
				
				// Credit Index - Transition Matrix DQ Bucket wise
				sqlContext.udf().register("creditIndex", new UDF3<Double, Double, WrappedArray<String>, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double credit_index, Double factor, WrappedArray<String> wPdnorm) throws Exception {

						double r = credit_index;
						double rp = credit_index;
						double f = factor;
						int index = 0;
						int n = wPdnorm.length();
						double[] rate = new double[n];
						double[] pdnorm = new double[n];
						double[][] trans_rate = new double[6][6];
						double[][] pd_norm_trans = new double[6][6];
						
						double pd_avg_dist_interim = 0.0;
						double[][] pd_avg_dist = new double[6][6];
						double[][] squared_error = new double[6][6];
						double sum_squared_error;
						double sum_squared_error_prev = 1.0;
						
						
						NormalDistribution nd = new NormalDistribution();
						
						String b[] = new String[wPdnorm.length()];
						wPdnorm.copyToArray(b);

						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							rate[index] = Double.valueOf(splitArray[0]).doubleValue();
							pdnorm[index] = Double.valueOf(splitArray[1]).doubleValue();
							
							++index;
						}
						
						int k = 0;
						
						for(int i=0; i<=5; i++) {
							for(int j=0; j<=5; j++) {
								
								trans_rate[i][j] = rate[k];
								pd_norm_trans[i][j] = pdnorm[k];
								k++;
							}
						}
						
						while(true) {
							sum_squared_error = 0.0;
							
							for(int i=5; i>=0; i--) {
								for(int j=5; j>=0; j--) {
									if(i == 0) {
										pd_avg_dist[i][j] = 0;
									}
									else {
										if(j == 0) {
											pd_avg_dist[i][j] = 1 - pd_avg_dist_interim;
										}
										else {
											pd_avg_dist[i][j] = nd.cumulativeProbability(pd_norm_trans[i][j] + r) - pd_avg_dist_interim;
										}
									}
									
									pd_avg_dist_interim = pd_avg_dist_interim + pd_avg_dist[i][j];
									
									squared_error[i][j] = Math.pow((trans_rate[i][j] - pd_avg_dist[i][j]), 2);
									sum_squared_error = sum_squared_error + squared_error[i][j];
									
								}
								
								pd_avg_dist_interim = 0;
							}
							
							if(sum_squared_error > sum_squared_error_prev || sum_squared_error <= 0.0001) {
								break;
							}
							
							rp = r;
							sum_squared_error_prev = sum_squared_error;
							
							r = Precision.round((r - f), 4);
							
						}
						
						return rp;
					}
				}, DataTypes.DoubleType);
				
				// Lifetime PD - Transition Matrix DQ Bucket wise
				sqlContext.udf().register("lifetimePD", new UDF3<Integer, Integer, WrappedArray<String>, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Integer previous_dq_bucket, Integer current_dq_bucket, WrappedArray<String> wAvDist) throws Exception {

						int prev = previous_dq_bucket;
						int curr = current_dq_bucket;
						int index = 0;
						int n = wAvDist.length();
						
						double[] prev_year_pd = new double[n];
						double[] avg_dist = new double[n];
						double[][] prev_lifetime_pd = new double[6][6];
						double[][] avg_distance = new double[6][6];
						
						double lifetime_pd_interim = 0.0;
						double[] lifetime_pd = new double[6];
						double lifetime_pd_final;
						
						String b[] = new String[wAvDist.length()];
						wAvDist.copyToArray(b);

						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							prev_year_pd[index] = Double.valueOf(splitArray[0]).doubleValue();
							avg_dist[index] = Double.valueOf(splitArray[1]).doubleValue();
							
							++index;
						}
						
						int k = 0;
						
						for(int i=0; i<=5; i++) {
							for(int j=0; j<=5; j++) {
								
								prev_lifetime_pd[i][j] = prev_year_pd[k];
								avg_distance[i][j] = avg_dist[k];
								k++;
							}
						}
					
						for(int i=0; i<=curr; i++) {
							if(prev == 5) {
								lifetime_pd[i] = 0;
							}
							else {
								if(i == 5) {
									lifetime_pd[i] = 1 - lifetime_pd_interim;
								}
								else {
									lifetime_pd[i] = prev_lifetime_pd[prev][0] * avg_distance[0][i] + 
											prev_lifetime_pd[prev][1] * avg_distance[1][i] +
											prev_lifetime_pd[prev][2] * avg_distance[2][i] +
											prev_lifetime_pd[prev][3] * avg_distance[3][i] +
											prev_lifetime_pd[prev][4] * avg_distance[4][i];
								}
							}
							
							lifetime_pd_interim = lifetime_pd_interim + lifetime_pd[i];
						}
						
						lifetime_pd_final = lifetime_pd[curr];
						return lifetime_pd_final;
					}
				}, DataTypes.DoubleType);
				
				// EIR Scenario 1 and 2 Calculation
				sqlContext.udf().register("eirV1",
						new UDF7<Double, Double, Double, WrappedArray<Double>, WrappedArray<String>, Double, String, Double>() {

							/** */
							private static final long serialVersionUID = 1L;

							@Override
							public Double call(Double loan_amt, Double fee_amt, Double seed, WrappedArray<Double> wTotRepayment,
									WrappedArray<String> wCashflowDate, Double target, String frequency) throws Exception {

								Object b[] = new Object[wTotRepayment.length()];
								wTotRepayment.copyToArray(b);
								Object d[] = new Object[wCashflowDate.length()];
								wCashflowDate.copyToArray(d);
								
								double[] cashflow = new double[wTotRepayment.length()];
								Date[] dates = new Date[wTotRepayment.length()];
								double[] opening = new double[wTotRepayment.length()];
								double[] closing = new double[wTotRepayment.length()];
								double[] interest = new double[wTotRepayment.length()];
								double[] bal_time = new double[wTotRepayment.length()];
								double sum_int_eir = 0.0d;
								double sum_bal_time = 0.0d;
								double r = seed;
								int index = 0;
								int duration = 1;

								for (int i = 0; i < b.length; i++) {
									Object object = b[i];
									String string = object.toString();
									double dub = Double.valueOf(string).doubleValue();
									cashflow[index] = dub;

									object = d[i];
									string = object.toString();
									dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
									++index;
								}
								
								if(frequency.equals("M")) {
									duration = 12;
								}
								else if(frequency.equals("Q")) {
									duration = 4;
								}
								else if(frequency.equals("H")) {
									duration = 2;
								}
								
								index = 0;
								while (true) {
									
									for (int i = 0; i < b.length; i++) {
										if (i == 0) {
											opening[i] = loan_amt - fee_amt;
											interest[i] = 0.0d;
											bal_time[i] = 0.0d;
											closing[i] = opening[i] - cashflow[i];
										} else {
											opening[i] = closing[i - 1];
											interest[i] = opening[i] * r / duration;
											bal_time[i] = opening[i] / duration;
											closing[i] = opening[i] + interest[i] - cashflow[i];
										}
									}

									sum_int_eir = 0.0d;
									sum_bal_time = 0.0d;
									for (int i = 0; i < cashflow.length; i++) {
										sum_int_eir += interest[i];
										sum_bal_time += bal_time[i];
									}

									if (index > 199) {
										logger.debug("Programs gonna break now due to too many iterations: " + r);
										break;
									}
									if (Double.isNaN(r)) {
										logger.debug("Programs gonna break now due to NaN : " + r);
										break;
									}
									if (Math.abs(sum_int_eir - target) < 0.000001) {
										System.out.println("closing balance is " + closing[b.length-1]);
										logger.debug("Programs gonna break now due to r : " + r);
										break;
									}
									
									r = r - (sum_int_eir - target) / (sum_bal_time);
									index++;
								}

								return r;
							}
						}, DataTypes.DoubleType);
				
				// Fee Amortization version2 (change in duration calc)
				sqlContext.udf().register("feeAmort2",
						new UDF7<Double, Double, Double, WrappedArray<Double>, WrappedArray<Double>, WrappedArray<String>, String, Double>() {

							/** */
							private static final long serialVersionUID = 1L;

							@Override
							public Double call(Double loan_amt, Double fee_amt, Double eir, WrappedArray<Double> wCashflow,
									WrappedArray<Double> cInterest, WrappedArray<String> wCashflowDate, String frequency) throws Exception {

								Object b[] = new Object[wCashflow.length()];
								wCashflow.copyToArray(b);
								Object c[] = new Object[cInterest.length()];
								cInterest.copyToArray(c);
								Object d[] = new Object[wCashflowDate.length()];
								wCashflowDate.copyToArray(d);

								double[] cashflow = new double[wCashflow.length()];
								Date[] dates = new Date[wCashflow.length()];
								double[] interest = new double[wCashflow.length()];
								double[] opening = new double[wCashflow.length()];
								double[] closing = new double[wCashflow.length()];
								double[] interestEir = new double[wCashflow.length()];
								double amortFee = 0.0d;
								int index = 0;
								int duration = 1;
								
								for (int i = 0; i < b.length; i++) {
									Object object = b[i];
									String string = object.toString();
									double dub = Double.valueOf(string).doubleValue();
									cashflow[index] = dub;

									object = c[i];
									string = object.toString();
									interest[index] = Double.valueOf(string).doubleValue();

									object = d[i];
									string = object.toString();
									dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
									++index;
								}

								if(frequency.equals("M")) {
									duration = 12;
								}
								else if(frequency.equals("Q")) {
									duration = 4;
								}
								else if(frequency.equals("H")) {
									duration = 2;
								}
								
								for (int i = 0; i < b.length; i++) {
									if (i == 0) {
										opening[i] = loan_amt - fee_amt;
										interestEir[i] = 0.0d;
										closing[i] = opening[i] - cashflow[i];
									} else {
										
										opening[i] = closing[i - 1];
										interestEir[i] = opening[i] / duration * eir;
										closing[i] = opening[i] + interestEir[i] - cashflow[i];
										amortFee = interestEir[i] - interest[i];
									}
								}

								return amortFee;
							}
						}, DataTypes.DoubleType);
				
				// Investment EIR
				sqlContext.udf().register("invEir",
						new UDF6<WrappedArray<Double>, WrappedArray<Double>, Double, WrappedArray<Double>, WrappedArray<String>, Double, Double>() {

							/** */
							private static final long serialVersionUID = 1L;

							@Override
							public Double call(WrappedArray<Double> wTotalPrice, WrappedArray<Double> wInterestAmount, Double seed, WrappedArray<Double> wDuration,
									WrappedArray<String> wCashflowDate, Double target) throws Exception {

								Object b[] = new Object[wTotalPrice.length()];
								wTotalPrice.copyToArray(b);
								Object c[] = new Object[wInterestAmount.length()];
								wInterestAmount.copyToArray(c);
								Object d[] = new Object[wCashflowDate.length()];
								wCashflowDate.copyToArray(d);
								Object e[] = new Object[wDuration.length()];
								wDuration.copyToArray(e);
								
								double[] price = new double[wCashflowDate.length()];
								Date[] dates = new Date[wCashflowDate.length()];
								double[] interest = new double[wCashflowDate.length()];
								double[] cashflow = new double[wCashflowDate.length()];
								double[] duration = new double[wCashflowDate.length()];
								double[] eff_int_amt = new double[wCashflowDate.length()];
								
								double r = seed;
								int index = 0;
								double finalBalance = 0.0d;
								
								int iteration = 0;
								int nonValue = 0;
								
								for (int i = 0; i < b.length; i++) {
									Object object = b[i];
									String string = object.toString();
									double dub = Double.valueOf(string).doubleValue();
									price[index] = dub;
									
									object = c[i];
									string = object.toString();
									dub = Double.valueOf(string).doubleValue();
									interest[index] = dub;
									
									object = d[i];
									string = object.toString();
									dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
									
									object = e[i];
									string = object.toString();
									dub = Double.valueOf(string).doubleValue();
									duration[index] = dub;
									
									++index;
								}
								
								
								index = 0;
								while (true) {
									
									for (int i = 0; i < b.length; i++) {
										if (i == 0) {
											
											eff_int_amt[i] = 0.0d;
											cashflow[i] = price[i];
											
										} else {
											eff_int_amt[i] = cashflow[i-1] * r * duration[i];
											cashflow[i] = cashflow[i-1] + eff_int_amt[i] - interest[i] + price[i];
												
										}
										finalBalance = cashflow[i];
										
									}
									
									if (index > 9999999) {
										logger.debug("Programs gonna break now due to too many iterations while increasing the rate: " + r);
										iteration = 1;
										break;
									}
									if (Double.isNaN(r)) {
										logger.debug("Programs gonna break now due to NaN : " + r);
										nonValue = 1;
										break;
									}
									if (Precision.round(finalBalance,2) == target || (finalBalance >= (target - target * 0.0001) && finalBalance <= (target + target * 0.0001))) {
										logger.debug("Programs gonna break now due to r : " + r);
										break;
									}
									
									r = Precision.round(r + 0.00000001 , 8);
									
									index++;
								}
								
								
								if(iteration == 1 || nonValue == 1) {
									index = 0;
									r = seed;
									while (true) {
										
										for (int i = 0; i < b.length; i++) {
											if (i == 0) {
												
												eff_int_amt[i] = 0.0d;
												cashflow[i] = price[i];
												
											} else {
												eff_int_amt[i] = cashflow[i-1] * r * duration[i];
												cashflow[i] = cashflow[i-1] + eff_int_amt[i] - interest[i] + price[i];
												
											}
											finalBalance = cashflow[i];
										}
										
										
										if (index > 9999999) {
											logger.debug("Programs gonna break now due to too many iterations while decreasing the rate: " + r);
											iteration = 1;
											break;
										}
										if (Double.isNaN(r)) {
											logger.debug("Programs gonna break now due to NaN : " + r);
											nonValue = 1;
											break;
										}
										if (Precision.round(finalBalance,2) == target || (finalBalance >= (target - target * 0.0001) && finalBalance <= (target + target * 0.0001))) {
															logger.debug("Programs gonna break now due to r : " + r);
											break;
										}
										
										r = Precision.round(r - 0.00000001 , 8);
										
										index++;
									}
								}
								
								return r;
							}
						}, DataTypes.DoubleType);
				
				// Investment EIR - Amount
				sqlContext.udf().register("invEirAmt",
						new UDF5<WrappedArray<Double>, WrappedArray<Double>, Double, WrappedArray<Double>, WrappedArray<String>, Double>() {

							/** */
							private static final long serialVersionUID = 1L;

							@Override
							public Double call(WrappedArray<Double> wTotalPrice, WrappedArray<Double> wInterestAmount, Double eir, WrappedArray<Double> wDuration,
									WrappedArray<String> wCashflowDate) throws Exception {

								Object b[] = new Object[wTotalPrice.length()];
								wTotalPrice.copyToArray(b);
								Object c[] = new Object[wInterestAmount.length()];
								wInterestAmount.copyToArray(c);
								Object d[] = new Object[wCashflowDate.length()];
								wCashflowDate.copyToArray(d);
								Object e[] = new Object[wDuration.length()];
								wDuration.copyToArray(e);
								
								double[] price = new double[wCashflowDate.length()];
								Date[] dates = new Date[wCashflowDate.length()];
								double[] interest = new double[wCashflowDate.length()];
								double[] cashflow = new double[wCashflowDate.length()];
								double[] duration = new double[wCashflowDate.length()];
								double[] eff_int_amt = new double[wCashflowDate.length()];
								
								int index = 0;
								double balance = 0.0d;
								
								for (int i = 0; i < b.length; i++) {
									Object object = b[i];
									String string = object.toString();
									double dub = Double.valueOf(string).doubleValue();
									price[index] = dub;
									
									object = c[i];
									string = object.toString();
									dub = Double.valueOf(string).doubleValue();
									interest[index] = dub;
									
									object = d[i];
									string = object.toString();
									dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
									
									object = e[i];
									string = object.toString();
									dub = Double.valueOf(string).doubleValue();
									duration[index] = dub;
									
									++index;
								}
								
								
								index = 0;
								
								for (int i = 0; i < b.length; i++) {
										if (i == 0) {
											
											eff_int_amt[i] = 0.0d;
											cashflow[i] = price[i];
											
										} else {
											eff_int_amt[i] = cashflow[i-1] * eir * duration[i];
											cashflow[i] = cashflow[i-1] + eff_int_amt[i] - interest[i] + price[i];
												
										}
										
										index++;
									}
								
								balance = cashflow[index-1];
								
								return balance;
							}
						}, DataTypes.DoubleType);
				
				// Fair Value RD Principle Amount Calculation (SIB)
				sqlContext.udf().register("rdPrinAmt", new UDF5<WrappedArray<String>, WrappedArray<Double>, Double, WrappedArray<String>, String, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wCashflowDate, WrappedArray<Double> wEmi, Double intRate, WrappedArray<String> wDurationFlag, String reportingDate) throws Exception {
						
						Object b[] = new Object[wEmi.length()];
						wEmi.copyToArray(b);
						Object c[] = new Object[wCashflowDate.length()];
						wCashflowDate.copyToArray(c);
						Object d[] = new Object[wDurationFlag.length()];
						wDurationFlag.copyToArray(d);
						
						
						Double[] emi = new Double[b.length];
						Date[] dates = new Date[b.length];
						Double[] interest = new Double[b.length];
						Double[] balance = new Double[b.length];
						Double[] prinAmt = new Double[b.length];
						String[] flag = new String[b.length];
						
						Double rate = intRate;
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							emi[index] = dub;
							
							object = c[i];
							string = object.toString();
							dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
							
							object = d[i];
							string = object.toString();
							flag[index] = string;
							
							++index;
						}
						
						Object object = reportingDate;
						String string = object.toString();
						Date reporting = new SimpleDateFormat("yyyy-MM-dd").parse(string);
						
						for(int i=0; i < b.length; i++) {
								if(i == 0) {
									prinAmt[i] = emi[i];
									interest[i] = prinAmt[i] * rate / 12;
									balance[i] = prinAmt[i] + interest[i];
								}
								else if(flag[i].equals("Y")) {
									double duration = TimeUnit.MILLISECONDS
											.toDays(dates[i].getTime() - dates[i - 1].getTime());
									
									if(dates[i].equals(reporting)) {
										prinAmt[i] = balance[i-1] + emi[i];
										interest[i] = prinAmt[i] * rate * duration / 365;
										balance[i] = prinAmt[i] + interest[i];
									}
									else {
										prinAmt[i] = balance[i-2] + emi[i];
										interest[i] = prinAmt[i] * rate * duration / 365;
										balance[i] = prinAmt[i] + interest[i] + interest[i-1];
									}
									
								}
								else {
									prinAmt[i] = balance[i-1] + emi[i];
									interest[i] = prinAmt[i] * rate / 12;
									balance[i] = prinAmt[i] + interest[i];
								}
						}
											
						Double result = prinAmt[index-1];
						
						return result;
					}
				}, DataTypes.DoubleType);
				
				// Fair Value RD EMI for Market Rate Calculation (SIB)
				sqlContext.udf().register("mktEmi",
						new UDF6<WrappedArray<String>, Double, Double, Double, WrappedArray<String>, String, Double>() {

							/** */
							private static final long serialVersionUID = 1L;

							@Override
							public Double call(WrappedArray<String> wCashflowDate, Double seed, Double mkt_rate, Double target, WrappedArray<String> wDurationFlag, String reportingDate) throws Exception {

								Object b[] = new Object[wCashflowDate.length()];
								wCashflowDate.copyToArray(b);
								String c[] = new String[wDurationFlag.length()];
								wDurationFlag.copyToArray(c);
								
								double[] prin_amt = new double[b.length];
								double[] interest = new double[b.length];
								double[] balance = new double[b.length];
								Date[] dates = new Date[b.length];
								String[] flag = new String[b.length];
								
								double finalBalance = 0.0d;
								double r = seed;
								double mkt_int_rate = mkt_rate;
								
								int index = 0;
								
								for (int i = 0; i < b.length; i++) {
									
									Object object = b[i];
									String string = object.toString();
									dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
									
									
									String[] splitArray = c[i].split("\\|");														
									flag[index] = splitArray[1];
										
									
									++index;
								}
								
								Object object = reportingDate;
								String string = object.toString();
								Date reporting = new SimpleDateFormat("yyyy-MM-dd").parse(string);
								
								index = 0;
								
								while (true) {
									
									for (int i = 0; i < b.length; i++) {
										if (i == 0) {
											
											prin_amt[i] = r;
											interest[i] = prin_amt[i] * mkt_int_rate / 12;
											balance[i] = prin_amt[i] + interest[i];
											
										}
										else if(flag[i].equals("Y")) {
											double duration = TimeUnit.MILLISECONDS
													.toDays(dates[i].getTime() - dates[i - 1].getTime());
											if(dates[i].equals(reporting)) {
												
												prin_amt[i] = balance[i-1] + r;
												interest[i] = prin_amt[i] * mkt_int_rate * duration / 365;
												balance[i] = prin_amt[i] + interest[i];
											}
											else {
												prin_amt[i] = balance[i-2] + r;
												interest[i] = prin_amt[i] * mkt_int_rate * duration / 365;
												balance[i] = prin_amt[i] + interest[i] + interest[i-1];
											}
											
										}
										else {
											prin_amt[i] = balance[i-1] + r;
											interest[i] = prin_amt[i] * mkt_int_rate / 12;
											balance[i] = prin_amt[i] + interest[i];
													
										}
										
										finalBalance = balance[i];
									}

									if (index > 999999) {
										logger.debug("Programs gonna break now due to too many iterations: " + r);
										break;
									}
									if (Double.isNaN(r)) {
										logger.debug("Programs gonna break now due to NaN : " + r);
										break;
									}
									if (Precision.round(finalBalance,2) == target || (finalBalance >= (target - 1) && finalBalance <= (target + 1))) {
									logger.debug("Programs gonna break now due to r : " + r);
										break;
									}
									
									r = Precision.round(r + 0.01 , 2);
									index++;
								}
								
								
								return r;
							}
						}, DataTypes.DoubleType);
				
				// Register LN (Natural Log)
				sqlContext.udf().register("nLog", new UDF1<Double, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double p) throws Exception {

						Log nl = new Log();
						return nl.value(p);
					}
				}, DataTypes.DoubleType);
				
				// Fair Value Education & Festival Loans Principle Amount Calculation (SIB)
				sqlContext.udf().register("osPrinAmt", new UDF3<Double, Double, WrappedArray<Double>,Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double disbursedAmt, Double intRate, WrappedArray<Double> wRepayment) throws Exception {
						
						
						Object b[] = new Object[wRepayment.length()];
						wRepayment.copyToArray(b);
						
						Double[] disbAmt = new Double[b.length];
						Double[] repayment = new Double[b.length];
						Double[] interest = new Double[b.length];
						Double[] balance = new Double[b.length];
						Double rate = intRate;
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							repayment[index] = dub;
							
							++index;
						}
						
						
						for(int i=0; i < b.length; i++) {
								if(i == 0) {
									interest[i] = 0.0;
									balance[i] = disbursedAmt;
								}
								else {
									disbAmt[i] = balance[i-1];
									interest[i] = disbAmt[i] * rate;
									balance[i] = disbAmt[i] + interest[i] - repayment[i];
								}
						}
											
						Double result = balance[index-1];
						
						return result;
					}
				}, DataTypes.DoubleType);
				
				// ABFL NPV EMI
				sqlContext.udf().register("npvEmi",
						new UDF5<WrappedArray<String>, Double, Double, Double, WrappedArray<String>, Double>() {

							/** */
							private static final long serialVersionUID = 1L;

							@Override
							public Double call(WrappedArray<String> wCashflowDate, Double nominal, Double seed, Double interest_rate, WrappedArray<String> wDurationFlag) throws Exception {

								Object b[] = new Object[wCashflowDate.length()];
								wCashflowDate.copyToArray(b);
								String c[] = new String[wDurationFlag.length()];
								wDurationFlag.copyToArray(c);
								
								double[] prin_amt = new double[b.length];
								double[] interest = new double[b.length];
								double[] balance = new double[b.length];
								Date[] dates = new Date[b.length];
								String[] flag = new String[b.length];
								
								double finalBalance = 0.0d;
								double r = seed;
								double int_rate = interest_rate;
								double initial_bal = nominal;
								
								
								int index = 0;
								
								for (int i = 0; i < b.length; i++) {
									
									Object object = b[i];
									String string = object.toString();
									dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
									
									
									String[] splitArray = c[i].split("\\|");														
									flag[index] = splitArray[1];
										
									
									++index;
								}
								
								index = 0;
								
								while (true) {
									
									for (int i = 0; i < b.length; i++) {
										if (i == 0) {
											
											prin_amt[i] = initial_bal;
											interest[i] = prin_amt[i] * int_rate / 12;
											balance[i] = prin_amt[i] + interest[i] - r;
											
										}
										else if(flag[i].equals("Y")) {
											double duration = TimeUnit.MILLISECONDS
													.toDays(dates[i].getTime() - dates[i - 1].getTime());
											
												prin_amt[i] = balance[i-1];
												interest[i] = prin_amt[i] * int_rate * duration / 365;
												balance[i] = prin_amt[i] + interest[i] - r;
												
										}
										else {
											prin_amt[i] = balance[i-1];
											interest[i] = prin_amt[i] * int_rate / 12;
											balance[i] = prin_amt[i] + interest[i] - r;
													
										}
										
										finalBalance = balance[i];
									}

									if (index > 999999) {
										logger.debug("Programs gonna break now due to too many iterations: " + r);
										break;
									}
									if (Double.isNaN(r)) {
										logger.debug("Programs gonna break now due to NaN : " + r);
										break;
									}
									if (Precision.round(finalBalance,0) == 0 || (finalBalance <= 1 && finalBalance >= -1)) {
									logger.debug("Programs gonna break now due to r : " + r);
										break;
									}
									
									r = Precision.round(r + 0.1 , 1);
									index++;
								}
								
								
								return r;
							}
						}, DataTypes.DoubleType);
				
				// Register Slope
				sqlContext.udf().register("slope", new UDF2<WrappedArray<Double>, WrappedArray<Double>, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<Double> independent, WrappedArray<Double> dependent) throws Exception {
						
						SimpleRegression regression = new SimpleRegression();
						
						double slope = 0;
						
						double x[] = new double[independent.length()];
						double y[] = new double[dependent.length()];
						
						Object b[] = new Object[independent.length()];
						independent.copyToArray(b);
						Object c[] = new Object[dependent.length()];
						dependent.copyToArray(c);
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							x[index] = dub;
							
							Object object1 = c[i];
							String string1 = object1.toString();
							double dub1 = Double.valueOf(string1).doubleValue();
							y[index] = dub1;
							
							++index;
						}
						
						for (int i = 0; i < x.length; i++) {
							regression.addData(x[i], y[i]);
						} 
						
						slope = regression.getSlope();
						return slope; 
					}
				}, DataTypes.DoubleType);
				
				// Register Intercept
				sqlContext.udf().register("intercept", new UDF2<WrappedArray<Double>, WrappedArray<Double>, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<Double> independent, WrappedArray<Double> dependent) throws Exception {
						
						SimpleRegression regression = new SimpleRegression();
						
						double intercept = 0;
						
						double x[] = new double[independent.length()];
						double y[] = new double[dependent.length()];
						
						Object b[] = new Object[independent.length()];
						independent.copyToArray(b);
						Object c[] = new Object[dependent.length()];
						dependent.copyToArray(c);
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							x[index] = dub;
							
							Object object1 = c[i];
							String string1 = object1.toString();
							double dub1 = Double.valueOf(string1).doubleValue();
							y[index] = dub1;
							
							++index;
						}
						
						for (int i = 0; i < x.length; i++) {
							regression.addData(x[i], y[i]);
						} 
						
						intercept = regression.getIntercept();
						return intercept; 
					}
				}, DataTypes.DoubleType);
				
				// Alpha - PD Optimized Default Rate
				sqlContext.udf().register("alpha1", new UDF7<Double, Double, Double, WrappedArray<Double>, WrappedArray<Double>, WrappedArray<Double>, Double, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double seed, Double beta, Double direction, WrappedArray<Double> wPre_ttc, WrappedArray<Double> wRating_prop, WrappedArray<Double> wDefault_rating_flg, Double default_rate) throws Exception {

						double r = seed;
						int index = 0;
						int n = wPre_ttc.length();
						
						Object b[] = new Object[wPre_ttc.length()];
						wPre_ttc.copyToArray(b);
						Object c[] = new Object[wPre_ttc.length()];
						wRating_prop.copyToArray(c);
						Object d[] = new Object[wPre_ttc.length()];
						wDefault_rating_flg.copyToArray(d);
						
						double[] pre_ttc = new double[n];
						double[] prop = new double[n];
						double[] default_rating_flg = new double[n];
						
						double[] adjusted_ln_odds = new double[n];
						double[] calibrated_ttc = new double[n];
						double[] adjusted_sum_product = new double[n];
						
						double adjusted_dr = 0.0;
						double error = 0.0;
						double factor = 0.0000001 * direction;
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Precision.round(Double.valueOf(string).doubleValue(),16);
							pre_ttc[index] = dub;
							
							Object object1 = c[i];
							String string1 = object1.toString();
							double dub1 = Precision.round(Double.valueOf(string1).doubleValue(),16);
							prop[index] = dub1;
							
							Object object2 = d[i];
							String string2 = object2.toString();
							double dub2 = Double.valueOf(string2).doubleValue();
							default_rating_flg[index] = dub2;
							
							++index;
						}
						
						index = 0;
						
						while(true) {
							adjusted_dr = 0.0;
							
							for(int i=0; i<n; i++) {
								
								adjusted_ln_odds[i] = Precision.round((r + beta * pre_ttc[i]),16);
								if(default_rating_flg[i] == 1) {
									calibrated_ttc[i] = 1;
									adjusted_sum_product[i] = 0;
								}
								else {
									calibrated_ttc[i] = Precision.round((1 / (1 + Math.exp(-adjusted_ln_odds[i]))),16);
									adjusted_sum_product[i] = Precision.round((prop[i] * calibrated_ttc[i]),16);
									adjusted_dr = Precision.round((adjusted_dr + adjusted_sum_product[i]),16); 
								}
							}
							
							error = Precision.round((adjusted_dr - default_rate),4);
							
							if (index > 9999999) {
								logger.debug("Programs gonna break now due to too many iterations: " + r);
								break;
							}
							if (Double.isNaN(r)) {
								logger.debug("Programs gonna break now due to NaN : " + r);
								break;
							}
							if(error == 0 || (error < 0.0001 && error > -0.0001)) {
								break;
							}
							
							r = Precision.round((r + factor), 7);
							index++;
						}
						
						return r;
					}
				}, DataTypes.DoubleType);
	
				// Alpha - PD Optimized Predicted Default Rate
				sqlContext.udf().register("alpha2", new UDF6<Double, Double, Double, WrappedArray<Double>, WrappedArray<Double>, Double, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double seed, Double beta_original, Double direction, WrappedArray<Double> rating_prop, WrappedArray<Double> cutoff_score, Double default_rate_predicted) throws Exception {

						double r = Precision.round(seed,5);
						double beta = Precision.round(beta_original,16);
						
						int index = 0;
						
						Object b[] = new Object[rating_prop.length()];
						rating_prop.copyToArray(b);
						Object c[] = new Object[cutoff_score.length()];
						cutoff_score.copyToArray(c);
						
						double[] prop = new double[b.length];
						double[] cutoff = new double[c.length];
						
						Double dr_predicted = Precision.round(default_rate_predicted,16);
						
						double[] model_output = new double[b.length];
						double[] dr_model = new double[b.length];
						double[] defaults = new double[b.length];
						double dr_realized = 0.0;
						
						double error = 0.0;
						double factor = 0.00001 * direction;
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Precision.round(Double.valueOf(string).doubleValue(),16);
							prop[index] = dub;
							
							Object object1 = c[i];
							String string1 = object1.toString();
							double dub1 = Precision.round(Double.valueOf(string1).doubleValue(),16);
							cutoff[index] = dub1;
							
							++index;
						}
						
						index = 0;
						
						while(true) {
							
							dr_realized = 0.0;
							
							for(int i=0 ; i < b.length; i++) {
								model_output[i] = Precision.round(r + beta * cutoff[i],16);
								dr_model[i] = Precision.round(Math.exp(model_output[i])/(1 + Math.exp(model_output[i])),16);
								defaults[i] = Precision.round(dr_model[i] * prop[i],16);
								dr_realized += defaults[i];
								
							}
							
							error = Precision.round((dr_predicted - dr_realized),4);
							
							if (index > 999999) {
								logger.debug("Programs gonna break now due to too many iterations: " + r);
								break;
							}
							if (Double.isNaN(r)) {
								logger.debug("Programs gonna break now due to NaN : " + r);
								break;
							}
							if(error == 0 || (error < 0.00001 && error > -0.00001)) {
								break;
							}
							
							r = Precision.round((r + factor), 5);
							index++;
						}
						
						return r;
					}
				}, DataTypes.DoubleType);
				
				// Exposure - (EAD-Impaired)
				sqlContext.udf().register("expImp", new UDF3<Integer, Integer, WrappedArray<String>, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Integer predYearCount, Integer forecastingCount, WrappedArray<String> wPredForePitDiffEAD) throws Exception {

						int index = 0;
						int p_count = predYearCount;
						int f_count = forecastingCount;
						
						String b[] = new String[wPredForePitDiffEAD.length()];
						wPredForePitDiffEAD.copyToArray(b);
						
						String[] predYear = new String[b.length];
						String[] forecasting = new String[b.length];
						Double[] pit_adj = new Double[b.length];
						Double[] days_diff = new Double[b.length];
						Double[] predicted_ead = new Double[b.length];
						Double[] exposure_final = new Double[b.length];
						
						String[][] prediction = new String[f_count][p_count];
						String[][] forecast = new String[f_count][p_count];
						Double[][] pit = new Double[f_count][p_count];
						Double[][] diff = new Double[f_count][p_count];
						Double[][] ead = new Double[f_count][p_count];
						
						Double[][] impaired = new Double[f_count][p_count];
						Double[][] exposure = new Double[f_count][p_count];
						
						Double[] previous_impaired = new Double[f_count];
						
						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							predYear[index] = splitArray[0];
							forecasting[index] = splitArray[1];
							pit_adj[index] = Double.valueOf(splitArray[2]).doubleValue();
							days_diff[index] = Double.valueOf(splitArray[3]).doubleValue();
							predicted_ead[index] = Double.valueOf(splitArray[4]).doubleValue();
							
							++index;
						}
						
						int k = 0;
						
						for(int i=0 ; i < f_count; i++) {
							for(int j=0; j < p_count; j++) {
								forecast[i][j] = forecasting[k];
								prediction[i][j] = predYear[k];
								pit[i][j] = pit_adj[k];
								diff[i][j] = days_diff[k];
								ead[i][j] = predicted_ead[k];
								
								k++;
								
								if(k == index || !forecast[i][j].equals(forecasting[k])) {
									break;
								}
							}
						}
						
						k = 0;
						
						for(int i=0; i < f_count; i++) {
							if(i == 0) {
								previous_impaired[i] = 0.0;
							}
							else {
								previous_impaired[i] = previous_impaired[i-1];
							}
							for(int j=0; j < p_count; j++) {
								if(i==0) {
									exposure[i][j] = Math.max(ead[i][j] , 0);
									impaired[i][j] = exposure[i][j] * pit[i][j];
									if(diff[i][j] < 365) {
										previous_impaired[i] += impaired[i][j];
									}
								}
								else {
									exposure[i][j] = Math.max(ead[i][j] - previous_impaired[i-1] , 0);
									impaired[i][j] = exposure[i][j] * pit[i][j];
									if(diff[i][j] < 365) {
										previous_impaired[i] += impaired[i][j];
									}
								}
								exposure_final[k] = exposure[i][j];
								k++;
								if(k == index || !forecast[i][j].equals(forecasting[k])) {
									break;
								}
							}
						}
						
						return exposure_final[index-1];
					}
				}, DataTypes.DoubleType);
				
				// Retail Stock Flow Exposure
				sqlContext.udf().register("expRet", new UDF4<Integer, Integer, WrappedArray<String>, String, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Integer predYearCount, Integer forecastingCount, WrappedArray<String> wInputFields, String reporting_date) throws Exception {

						int index = 0;
						int p_count = predYearCount;
						int f_count = forecastingCount;
						int s_count;
						
						String b[] = new String[wInputFields.length()];
						wInputFields.copyToArray(b);
						
						if(b.length < 3) {
							s_count = b.length;
						}
						else {
							s_count = 3;
						}
						
						String[] predYear = new String[b.length];
						String[] forecasting = new String[b.length];
						Double[] days_diff = new Double[b.length];
						Double[] predicted_ead = new Double[b.length];
						int[] curr_stage = new int[b.length];
						Double[] stage0_pct = new Double[b.length];
						Double[] stage1_pct = new Double[b.length];
						Double[] stage2_pct = new Double[b.length];
						Double[] stage3_pct = new Double[b.length];
						
						Double[] exposure_final = new Double[b.length];
						
						String[][][] prediction = new String[f_count][p_count][s_count];
						String[][][] forecast = new String[f_count][p_count][s_count];
						Double[][][] diff = new Double[f_count][p_count][s_count];
						Double[][][] ead = new Double[f_count][p_count][s_count];
						int[][][] curr_stg = new int[f_count][p_count][s_count];
						Double[][][] stg0 = new Double[f_count][p_count][s_count];
						Double[][][] stg1 = new Double[f_count][p_count][s_count];
						Double[][][] stg2 = new Double[f_count][p_count][s_count];
						Double[][][] stg3 = new Double[f_count][p_count][s_count];
						
						Double[][][] stg0_mig = new Double[f_count][p_count][s_count];
						Double[][][] stg1_mig = new Double[f_count][p_count][s_count];
						Double[][][] stg2_mig = new Double[f_count][p_count][s_count];
						Double[][][] stg3_mig = new Double[f_count][p_count][s_count];
						
						Double[][][] exposure = new Double[f_count][p_count][s_count];
						
						Double[][] stg1_tot_mig = new Double[f_count][p_count];
						Double[][] stg2_tot_mig = new Double[f_count][p_count];
						Double[][] stg3_tot_mig = new Double[f_count][p_count];
						
						Double[] prev_closure_stg1 = new Double[f_count];
						Double[] prev_closure_stg2 = new Double[f_count];
						Double[] prev_closure_stg3 = new Double[f_count];
						
						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							predYear[index] = splitArray[0];
							forecasting[index] = splitArray[1];
							days_diff[index] = Double.valueOf(splitArray[2]).doubleValue();
							predicted_ead[index] = Double.valueOf(splitArray[3]).doubleValue();
							curr_stage[index] = Integer.valueOf(splitArray[4]).intValue();
							stage0_pct[index] = Double.valueOf(splitArray[5]).doubleValue();
							stage1_pct[index] = Double.valueOf(splitArray[6]).doubleValue();
							stage2_pct[index] = Double.valueOf(splitArray[7]).doubleValue();
							stage3_pct[index] = Double.valueOf(splitArray[8]).doubleValue();
							
							++index;
						}
						
						int k = 0;
						
						for(int i=0 ; i < f_count; i++) {
							for(int j=0; j < p_count; j++) {
								for(int s=0; s < s_count; s++) {
									forecast[i][j][s] = forecasting[k];
									prediction[i][j][s] = predYear[k];
									diff[i][j][s] = days_diff[k];
									ead[i][j][s] = predicted_ead[k];
									curr_stg[i][j][s] = curr_stage[k];
									stg0[i][j][s] = stage0_pct[k];
									stg1[i][j][s] = stage1_pct[k];
									stg2[i][j][s] = stage2_pct[k];
									stg3[i][j][s] = stage3_pct[k];
									
									k++;
									
									if(k == index) {
										break;
									}
								}
								if(k == index || !forecast[i][j][0].equals(forecasting[k])) {
									break;
								}
							}
						}
						
						k = 0;
						
						for(int i=0; i < f_count; i++) {
							
							prev_closure_stg1[i] = 0.0;
							prev_closure_stg2[i] = 0.0;
							prev_closure_stg3[i] = 0.0;
							
							for(int j=0; j < p_count; j++) {
								stg1_tot_mig[i][j] = 0.0;
								stg2_tot_mig[i][j] = 0.0;
								stg3_tot_mig[i][j] = 0.0;
										
								for(int s=0; s < s_count; s++) {
									if(prediction[i][j][s].equals(reporting_date)) {
										exposure[i][j][s] = ead[i][j][s];
									}
									else {
										if(prediction[i][j][s].equals(forecast[i][j][s])) {
											if(curr_stg[i][j][s] == 1) {
												exposure[i][j][s] = stg1_tot_mig[i-1][j] + prev_closure_stg1[i-1];
											}
											if(curr_stg[i][j][s] == 2) {
												exposure[i][j][s] = stg2_tot_mig[i-1][j] + prev_closure_stg2[i-1];
											}
											if(curr_stg[i][j][s] == 3) {
												exposure[i][j][s] = stg3_tot_mig[i-1][j] + prev_closure_stg3[i-1];
											}
										}
										else {
											if(curr_stg[i][j][s] == 1 || curr_stg[i][j][s] == 2) {
												exposure[i][j][s] = stg1_mig[i][j-1][s] + stg2_mig[i][j-1][s];
											}
											else {
												exposure[i][j][s] = stg3_mig[i][j-1][s];
											}
										}
									}
									
									stg0_mig[i][j][s] = exposure[i][j][s] * stg0[i][j][s];
									stg1_mig[i][j][s] = exposure[i][j][s] * stg1[i][j][s];
									stg2_mig[i][j][s] = exposure[i][j][s] * stg2[i][j][s];
									stg3_mig[i][j][s] = exposure[i][j][s] * stg3[i][j][s];
									
									stg1_tot_mig[i][j] += stg1_mig[i][j][s];
									stg2_tot_mig[i][j] += stg2_mig[i][j][s];
									stg3_tot_mig[i][j] += stg3_mig[i][j][s];
									
									if(diff[i][j][s] < 365) {
										if(curr_stg[i][j][s] == 1) {
											prev_closure_stg1[i] += stg0_mig[i][j][s];
										}
										if(curr_stg[i][j][s] == 2) {
											prev_closure_stg2[i] += stg0_mig[i][j][s];
										}
										if(curr_stg[i][j][s] == 3){
											prev_closure_stg3[i] += stg0_mig[i][j][s];
										}
									}
									
									exposure_final[k] = exposure[i][j][s];
									k++;
									
									if(k == index) {
										break;
									}
								}
								
								if(k == index || !forecast[i][j][0].equals(forecasting[k])) {
									break;
								}
							}
						}
						
						return exposure_final[index-1];
					}
				}, DataTypes.DoubleType);

				// Credit Index - Transition Matrix Stage wise
				sqlContext.udf().register("creditIndex1", new UDF3<Double, Double, WrappedArray<String>, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double credit_index, Double factor, WrappedArray<String> wPdnorm) throws Exception {

						double r = credit_index;
						double rp = credit_index;
						double f = factor;
						int index = 0;
						int n = wPdnorm.length();
						double[] rate = new double[n];
						double[] pdnorm = new double[n];
						double[][] trans_rate = new double[3][4];
						double[][] pd_norm_trans = new double[3][4];
						
						double pd_avg_dist_interim = 0.0;
						double[][] pd_avg_dist = new double[3][4];
						double[][] squared_error = new double[3][4];
						double sum_squared_error;
						double sum_squared_error_prev = 1.0;
						
						
						NormalDistribution nd = new NormalDistribution();
						
						String b[] = new String[wPdnorm.length()];
						wPdnorm.copyToArray(b);

						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							rate[index] = Double.valueOf(splitArray[0]).doubleValue();
							pdnorm[index] = Double.valueOf(splitArray[1]).doubleValue();
							
							++index;
						}
						
						int k = 0;
						
						for(int i=0; i<=2; i++) {
							for(int j=0; j<=3; j++) {
								
								trans_rate[i][j] = rate[k];
								pd_norm_trans[i][j] = pdnorm[k];
								k++;
							}
						}
						
						while(true) {
							sum_squared_error = 0.0;
							
							for(int i=2; i>=0; i--) {
								for(int j=3; j>=0; j--) {
									if(j == 0) {
											pd_avg_dist[i][j] = 1 - pd_avg_dist_interim;
									}
									else {
											pd_avg_dist[i][j] = nd.cumulativeProbability(pd_norm_trans[i][j] + r) - pd_avg_dist_interim;
									}
									
									pd_avg_dist_interim = pd_avg_dist_interim + pd_avg_dist[i][j];
									
									squared_error[i][j] = Math.pow((trans_rate[i][j] - pd_avg_dist[i][j]), 2);
									sum_squared_error = sum_squared_error + squared_error[i][j];
									
								}
								
								pd_avg_dist_interim = 0;
							}
							
							if(sum_squared_error > sum_squared_error_prev || sum_squared_error <= 0.0001) {
								break;
							}
							
							rp = r;
							sum_squared_error_prev = sum_squared_error;
							
							r = Precision.round((r - f), 4);
							
						}
						
						return rp;
					}
				}, DataTypes.DoubleType);
				
				// Lifetime PD - Transition Matrix Stage wise
				sqlContext.udf().register("lifetimePD1", new UDF3<Integer, Integer, WrappedArray<String>, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Integer previous_dq_bucket, Integer current_dq_bucket, WrappedArray<String> wAvDist) throws Exception {

						int prev = previous_dq_bucket;
						int curr = current_dq_bucket;
						int index = 0;
						int n = wAvDist.length();
						
						double[] prev_year_pd = new double[n];
						double[] avg_dist = new double[n];
						double[][] prev_lifetime_pd = new double[3][4];
						double[][] avg_distance = new double[3][4];
						
						double[] lifetime_pd = new double[4];
						double lifetime_pd_final;
						
						String b[] = new String[wAvDist.length()];
						wAvDist.copyToArray(b);

						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							prev_year_pd[index] = Double.valueOf(splitArray[0]).doubleValue();
							avg_dist[index] = Double.valueOf(splitArray[1]).doubleValue();
							
							++index;
						}
						
						int k = 0;
						
						for(int i=0; i<=2; i++) {
							for(int j=0; j<=3; j++) {
								
								prev_lifetime_pd[i][j] = prev_year_pd[k];
								avg_distance[i][j] = avg_dist[k];
								k++;
							}
						}
					
						for(int i=0; i<=curr; i++) {
							
								lifetime_pd[i] = prev_lifetime_pd[prev][0] * avg_distance[0][i] + 
											prev_lifetime_pd[prev][1] * avg_distance[1][i] +
											prev_lifetime_pd[prev][2] * avg_distance[2][i] +
											prev_lifetime_pd[prev][3] * avg_distance[3][i];
											
								}
							
						lifetime_pd_final = lifetime_pd[curr];
						return lifetime_pd_final;
					}
				}, DataTypes.DoubleType);
				
				// Cumulative Product
				sqlContext.udf().register("product", new UDF1<WrappedArray<Double>, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<Double> factor) throws Exception {

						Object b[] = new Object[factor.length()];
						factor.copyToArray(b);
						
						double product = 1.0;
						
						double[] values = new double[factor.length()];
						int index = 0;
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							values[index] = dub;
							++index;
						}
						
						for(int i = 0; i < b.length; i++) {
							product = product * values[i];
						}
						
						return product;
					}
				}, DataTypes.DoubleType);
				
				// Exposure Rank - Sensitivity
				sqlContext.udf().register("expRank", new UDF2<String, WrappedArray<String>, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(String cust_id, WrappedArray<String> custDetails) throws Exception {

						String b[] = new String[custDetails.length()];
						custDetails.copyToArray(b);
						
						String[] cust = new String[b.length];
						Double[] exposure = new Double[b.length];
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							
							String[] splitArray = b[i].split("\\|");														
							cust[index] = splitArray[0];
							exposure[index] = Double.valueOf(splitArray[1]).doubleValue();
							
							++index;
						}
						
						long count = Arrays.asList(cust).stream().distinct().count();
						
						int c = (int)count;
						String[] custIdentifier = new String[c];
						Double[] custExp = new Double[c];
						
						int k = 0;
						
						for (int i = 0; i < c; i++) {
							
							custIdentifier[i] = cust[k];
							
							for(int j = 0; j < b.length; j++) {
								if(j == 0) {
									custExp[i] = exposure[k];
								}
								else {
									custExp[i] = custExp[i] + exposure[k];
								}
								
								k++;
								
								if(k == index) {
									break;
								}
								
								if (!cust[k].equals(cust[k-1])) {
									break;
								}
							}
						}	
						
						int[] order = new int[c];

					    for (int i = 0; i < c; i++) {
					        int pos = 0;
					        for (int j = 0; j < c; j++) {
					            if (custExp[j] > custExp[i]) {
					                pos++;
					            }
					        }
					        order[i] = pos + 1;
					    }
						
					    int rank = 0;
					    
					    for (int i = 0; i <= c; i++) {
					    	rank = order[i];
					    	if(custIdentifier[i].equals(cust_id))
					    		break;
					    }
					    
						return rank;
					}
				}, DataTypes.IntegerType);
				
				// Concatenate text across rows separated with comma
				sqlContext.udf().register("merge_text", new UDF1<WrappedArray<String>, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String call(WrappedArray<String> wArrayList) throws Exception {
						
						Object b[] = new Object[wArrayList.length()];
						wArrayList.copyToArray(b);
						
						String[] array_list = new String[b.length];
						String final_text = "";
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							array_list[index] = string;
							
							++index;
						}

						for(int i = 0; i < b.length; i++) {
							
							final_text = final_text.concat(array_list[i]);
								if(i == index-1) {
									break;
							}
								final_text = final_text.concat(",");
						}
						
						return final_text;
					}
				}, DataTypes.StringType);
				
				// Alpha - PD Optimized Default Rate (when the direction of alpha movement is not known)
				sqlContext.udf().register("alpha3", new UDF5<Double, Double, WrappedArray<Double>, WrappedArray<Double>, Double, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double seed, Double beta, WrappedArray<Double> wLogDefault, WrappedArray<Double> wNumCust, Double central_tendency_pd) throws Exception {

						double r = seed;
						int index = 0;
						int n = wLogDefault.length();
						int iteration = 0;
						int nonValue = 0;
						
						Object b[] = new Object[wLogDefault.length()];
						wLogDefault.copyToArray(b);
						Object c[] = new Object[wNumCust.length()];
						wNumCust.copyToArray(c);
						
						double[] logDef = new double[n];
						double[] numCust = new double[n];
						
						double[] adjusted_ln_odds = new double[n];
						double[] calibrated_ttc = new double[n];
						double[] adjusted_sum_product = new double[n];
						
						double adjusted_dr_sumprod = 0.0;
						double adjusted_dr_wt_avg= 0.0;
						double total_cust= 0.0;
						
						double error = 0.0;
						double factor = 0.0001;
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Precision.round(Double.valueOf(string).doubleValue(),16);
							logDef[index] = dub;
							
							Object object1 = c[i];
							String string1 = object1.toString();
							double dub1 = Precision.round(Double.valueOf(string1).doubleValue(),16);
							numCust[index] = dub1;
							
							++index;
						}
						
						index = 0;
						
						while(true) {
							adjusted_dr_sumprod = 0.0;
							total_cust = 0.0;
							
							for(int i=0; i<n; i++) {
								
								adjusted_ln_odds[i] = Precision.round((r + beta * logDef[i]),16);
								calibrated_ttc[i] = Precision.round((1 / (1 + Math.exp(-adjusted_ln_odds[i]))),16);
								adjusted_sum_product[i] = Precision.round((numCust[i] * calibrated_ttc[i]),16);
								adjusted_dr_sumprod = Precision.round((adjusted_dr_sumprod + adjusted_sum_product[i]),16); 
								total_cust = Precision.round((total_cust + numCust[i]),0); 
								
							}
							
							adjusted_dr_wt_avg = Precision.round((adjusted_dr_sumprod/total_cust),16); 
							error = Precision.round((adjusted_dr_wt_avg - central_tendency_pd),8);
							
							if (index > 39999) {
								logger.debug("Programs gonna break now due to too many iterations: " + r);
								iteration = 1;
								break;
							}
							if (Double.isNaN(r)) {
								logger.debug("Programs gonna break now due to NaN : " + r);
								nonValue = 1;
								break;
							}
							if(error == 0 || (error < 0.000001 && error > -0.000001)) {
								break;
							}
							
							r = Precision.round((r - factor), 4);
							index++;
							
						}
						
							if(iteration == 1 || nonValue == 1) {
								index = 0;
								r = seed;
								
								while(true) {
									adjusted_dr_sumprod = 0.0;
									total_cust = 0.0;
									
									for(int i=0; i<n; i++) {
										
										adjusted_ln_odds[i] = Precision.round((r + beta * logDef[i]),16);
										calibrated_ttc[i] = Precision.round((1 / (1 + Math.exp(-adjusted_ln_odds[i]))),16);
										adjusted_sum_product[i] = Precision.round((numCust[i] * calibrated_ttc[i]),16);
										adjusted_dr_sumprod = Precision.round((adjusted_dr_sumprod + adjusted_sum_product[i]),16); 
										total_cust = Precision.round((total_cust + numCust[i]),0); 
										
									}
									
									adjusted_dr_wt_avg = Precision.round((adjusted_dr_sumprod/total_cust),16); 
									error = Precision.round((adjusted_dr_wt_avg - central_tendency_pd),8);
									
									if (index > 19999) {
										logger.debug("Programs gonna break now due to too many iterations: " + r);
										break;
									}
									if (Double.isNaN(r)) {
										logger.debug("Programs gonna break now due to NaN : " + r);
										break;
									}
									if(error == 0 || (error < 0.000001 && error > -0.000001)) {
										break;
									}
									
									r = Precision.round((r + factor), 4);
									index++;
							}
						}
						
						return r;
					}
				}, DataTypes.DoubleType);
				

				// Alpha - PD Optimized Default Rate (when the direction of alpha movement is not known) - Replica of alpha3 with 5 decimals
				sqlContext.udf().register("alpha4", new UDF5<Double, Double, WrappedArray<Double>, WrappedArray<Double>, Double, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double seed, Double beta, WrappedArray<Double> wLogDefault, WrappedArray<Double> wNumCust, Double central_tendency_pd) throws Exception {

						double r = seed;
						int index = 0;
						int n = wLogDefault.length();
						int iteration = 0;
						int nonValue = 0;
						
						Object b[] = new Object[wLogDefault.length()];
						wLogDefault.copyToArray(b);
						Object c[] = new Object[wNumCust.length()];
						wNumCust.copyToArray(c);
						
						double[] logDef = new double[n];
						double[] numCust = new double[n];
						
						double[] adjusted_ln_odds = new double[n];
						double[] calibrated_ttc = new double[n];
						double[] adjusted_sum_product = new double[n];
						
						double adjusted_dr_sumprod = 0.0;
						//double adjusted_dr_wt_avg= 0.0;
						//double total_cust= 0.0;
						
						double error = 0.0;
						double factor = 0.00001;
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Precision.round(Double.valueOf(string).doubleValue(),16);
							logDef[index] = dub;
							
							Object object1 = c[i];
							String string1 = object1.toString();
							double dub1 = Precision.round(Double.valueOf(string1).doubleValue(),16);
							numCust[index] = dub1;
							
							++index;
						}
						
						index = 0;
						
						while(true) {
							adjusted_dr_sumprod = 0.0;
							//total_cust = 0.0;
							
							for(int i=0; i<n; i++) {
								
								adjusted_ln_odds[i] = Precision.round((r + beta * logDef[i]),16);
								calibrated_ttc[i] = Precision.round((1 / (1 + Math.exp(-adjusted_ln_odds[i]))),16);
								adjusted_sum_product[i] = Precision.round((numCust[i] * calibrated_ttc[i]),16);
								adjusted_dr_sumprod = Precision.round((adjusted_dr_sumprod + adjusted_sum_product[i]),16); 
								//total_cust = Precision.round((total_cust + numCust[i]),0); 
								
							}
							
							//adjusted_dr_wt_avg = Precision.round((adjusted_dr_sumprod/total_cust),16); 
							//error = Precision.round((adjusted_dr_wt_avg - central_tendency_pd),8);
							error = Precision.round((adjusted_dr_sumprod - central_tendency_pd),8);
							//System.out.println("for r = " + r + " error is " + error);
							
							if (index > 399999) {
								logger.debug("Programs gonna break now due to too many iterations: " + r);
								iteration = 1;
								break;
							}
							if (Double.isNaN(r)) {
								logger.debug("Programs gonna break now due to NaN : " + r);
								nonValue = 1;
								break;
							}
							if(error == 0 || (error < 0.000001 && error > -0.000001)) {
								break;
							}
							
							r = Precision.round((r - factor), 5);
							index++;
							
						}
						
							if(iteration == 1 || nonValue == 1) {
								index = 0;
								r = seed;
								
								while(true) {
									adjusted_dr_sumprod = 0.0;
									//total_cust = 0.0;
									
									for(int i=0; i<n; i++) {
										
										adjusted_ln_odds[i] = Precision.round((r + beta * logDef[i]),16);
										calibrated_ttc[i] = Precision.round((1 / (1 + Math.exp(-adjusted_ln_odds[i]))),16);
										adjusted_sum_product[i] = Precision.round((numCust[i] * calibrated_ttc[i]),16);
										adjusted_dr_sumprod = Precision.round((adjusted_dr_sumprod + adjusted_sum_product[i]),16); 
										//total_cust = Precision.round((total_cust + numCust[i]),0); 
										
									}
									
									//adjusted_dr_wt_avg = Precision.round((adjusted_dr_sumprod/total_cust),16); 
									//error = Precision.round((adjusted_dr_wt_avg - central_tendency_pd),8);
									error = Precision.round((adjusted_dr_sumprod - central_tendency_pd),8);
									//System.out.println("for r = " + r + " error is " + error);
									
									if (index > 199999) {
										logger.debug("Programs gonna break now due to too many iterations: " + r);
										break;
									}
									if (Double.isNaN(r)) {
										logger.debug("Programs gonna break now due to NaN : " + r);
										break;
									}
									if(error == 0 || (error < 0.000001 && error > -0.000001)) {
										break;
									}
									
									r = Precision.round((r + factor), 5);
									index++;
							}
						}
						
						return r;
					}
				}, DataTypes.DoubleType);
				
				// Matrix Multiplication
				sqlContext.udf().register("mMult", new UDF2<String, WrappedArray<Double>, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(String concatColValues, WrappedArray<Double> wRowValues) throws Exception {
						
						String colValues = concatColValues;
						int n = wRowValues.length();
						int index = 0;
						
						Object b[] = new Object[wRowValues.length()];
						wRowValues.copyToArray(b);
						
						double[] cols = new double[n];
						double[] rows = new double[n];
						double[] prod = new double[n];
						
						double sumprod = 0.0;
						
						String[] splitString = colValues.split("\\|");														
						for (int i = 0; i < n; i++) {
						cols[i] ="".equals(splitString[i])?0.00: Double.valueOf(splitString[i]).doubleValue();
						}
						
						for (int i = 0; i < b.length; i++) {
							
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							rows[index] = dub;
							
							++index;
						}
						
						for(int i = 0; i < n; i++) {
							prod[i] = cols[i] * rows[i];
							sumprod = sumprod + prod[i];
						}
						
						return sumprod;
					}
				}, DataTypes.DoubleType);
				
				// Standard Deviation
				sqlContext.udf().register("stdDevS", new UDF1<WrappedArray<Double>, Double>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<Double> arr) throws Exception {

						StandardDeviation sd = new StandardDeviation();
						
						Object b[] = new Object[arr.length()];
						arr.copyToArray(b);
						
						int index = 0;
						double[] arrValue = new double[b.length];
						
						for (int i = 0; i < b.length; i++) {
							
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							arrValue[index] = dub;
							
							++index;
						}
						return sd.evaluate(arrValue);
					}
				}, DataTypes.DoubleType);
				
				// Cumulative PD as a factor of sum and product of stage wise PDs
				sqlContext.udf().register("cumulativePD", new UDF1<WrappedArray<Double>, Double>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<Double> pit_pd) throws Exception {

						Object b[] = new Object[pit_pd.length()];
						pit_pd.copyToArray(b);
						
						int index = 0;
						double[] pitPD = new double[b.length];
						double[] cumPD = new double[b.length];
						
						for (int i = 0; i < b.length; i++) {
							
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							pitPD[index] = dub;
							
							++index;
						}
						
						for(int i= 0; i < b.length; i++) {
							if(i == 0) {
								cumPD[i] = pitPD[i]; 
							}
							else {
								cumPD[i] = cumPD[i-1] + (1 - cumPD[i-1]) * pitPD[i];
							}
						}
						return cumPD[index-1];
					}
				}, DataTypes.DoubleType);
				
				// ABFL Cashflow EMI
				sqlContext.udf().register("cashEmi",
						new UDF4<WrappedArray<String>, Double, Double, Double, Double>() {

							/** */
							private static final long serialVersionUID = 1L;

							@Override
							public Double call(WrappedArray<String> wCashflowDate, Double nominal, Double seed, Double interest_rate) throws Exception {

								Object b[] = new Object[wCashflowDate.length()];
								wCashflowDate.copyToArray(b);
								
								double[] prin_amt = new double[b.length];
								double[] prin_cf = new double[b.length];
								double[] emi = new double[b.length];
								double[] interest = new double[b.length];
								double[] balance = new double[b.length];
								Date[] dates = new Date[b.length];
								
								double finalBalance = 0.0d;
								double r = seed;
								double int_rate = interest_rate;
								double initial_bal = nominal;
								double factor;
								
								if(r <= 10000) {
									factor = 0.1;
								}
								else if(r <= 100000) {
									factor = 1;
								}
								else if(r <= 1000000) {
									factor = 10;
								}
								else {
									factor = 100;
								}
								
								double threshold = factor * 10;
								
								int index = 0;
								
								for (int i = 0; i < b.length; i++) {
									
									Object object = b[i];
									String string = object.toString();
									dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
									
									++index;
								}
								
								index = 0;
								
								while (true) {
									
									for (int i = 0; i < b.length; i++) {
										if (i == 0) {
											
											prin_amt[i] = initial_bal;
											emi[i] = 0;
											interest[i] = 0;
											prin_cf[i] = 0;
											balance[i] = prin_amt[i];
											
										}
										else if (i == 1) {
											
											prin_amt[i] = balance[i-1];
											emi[i] = r;
											interest[i] = prin_amt[i] * int_rate * TimeUnit.MILLISECONDS
													.toDays(dates[i].getTime() - dates[i - 1].getTime()) / 365;
											prin_cf[i] = emi[i] - interest[i];
											balance[i] = prin_amt[i] - prin_cf[i];
											
										}
										else {
											prin_amt[i] = balance[i-1];
											emi[i] = r * TimeUnit.MILLISECONDS
													.toDays(dates[i].getTime() - dates[i - 1].getTime()) / 365;
											interest[i] = prin_amt[i] * int_rate * TimeUnit.MILLISECONDS
													.toDays(dates[i].getTime() - dates[i - 1].getTime()) / 365;
											prin_cf[i] = emi[i] - interest[i];
											balance[i] = prin_amt[i] - prin_cf[i];
													
										}
										
										finalBalance = balance[i];
									}
									
									if (Precision.round(finalBalance,0) == 0 || (finalBalance <= threshold && finalBalance >= -1 * threshold)) {
									   //logger.debug("Programs gonna break now due to r : " + r);
										break;
									}
									if (Precision.round(finalBalance,0) < 0) {
										logger.debug("Programs gonna break now due to final balance < 0; r is " + r + " nominal is " + nominal + " interest rate is " + int_rate + " seed is " + seed);
											break;
									}
									if (index > 99999) {
										logger.debug("Programs gonna break now due to too many iterations: " + r + " nominal is " + nominal);
										break;
									}
									if (Double.isNaN(r)) {
										logger.debug("Programs gonna break now due to NaN : " + r);
										break;
									}
									
									r = Precision.round(r + factor, 1);
									index++;
								}
								
								
								return r;
							}
						}, DataTypes.DoubleType);
				
				// Opening Principal Calculation (ABFL Cashflow)
				sqlContext.udf().register("openingPrin", new UDF4<WrappedArray<String>, Double, Double, Double, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wCashflow, Double nominalExp, Double baseEmi, Double interest_rate ) throws Exception {
						
						String b[] = new String[wCashflow.length()];
						wCashflow.copyToArray(b);
							
						double rate = interest_rate;
						double emi = baseEmi;
						double nominal = nominalExp;
						Date[] dates = new Date[b.length];
						Double[] openingPrin = new Double[b.length];
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							
							Object object = b[i];
							String string = object.toString();
							dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
							
							++index;
						}
						
						
						for(int i=0; i < b.length; i++) {
								if(i == 0 || i == 1) {
									openingPrin[i] = nominal;
								}
								else {
											openingPrin[i] = openingPrin[i-1] - ((emi - (openingPrin[i-1] * rate)) * TimeUnit.MILLISECONDS
													.toDays(dates[i-1].getTime() - dates[i-2].getTime()) / 365);
									}
						}
											
						
						Double result = openingPrin[index-1];
						
						return result;
					}
				}, DataTypes.DoubleType);
				
				// Opening Principal Calculation (CBI Cashflow)
				sqlContext.udf().register("endingBal", new UDF4<WrappedArray<String>, Double, Double, Double, Double>() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wCashflow, Double nominalExp, Double baseEmi, Double repayment_rate ) throws Exception {
						
						String b[] = new String[wCashflow.length()];
						wCashflow.copyToArray(b);
							
						double rate = repayment_rate;
						double emi = baseEmi;
						double nominal = nominalExp;
						Date[] dates = new Date[b.length];
						Double[] endingBal = new Double[b.length];
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							
							Object object = b[i];
							String string = object.toString();
							dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
							
							++index;
						}
						
						
						for(int i=0; i < b.length; i++) {
							if(i == 0 ) {
								endingBal[i] = nominal-emi+(nominal*rate);
							}
							else {
								endingBal[i] = endingBal[i-1] - ((emi - (endingBal[i-1]*rate))) ;
								}
						}
											
						
						Double result = endingBal[index-1];
						
						return result;
					}
				}, DataTypes.DoubleType);
				
				
				// Opening EAD Calculation (CBI Predicted_EAD)
				sqlContext.udf().register("predictedEad", new UDF1<WrappedArray<String>, Double >() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wCashflow ) throws Exception {
						
						String b[] = new String[wCashflow.length()];
						wCashflow.copyToArray(b);
						
						Date[]  cashflowDate = new Date[b.length];
						Date[]  maturityDate = new Date[b.length];
						String[] fundedType = new String[b.length];
						String[] portfolioCode = new String[b.length];
						String[] productCode = new String[b.length];
						int[] stage = new int[b.length];
						Double[] nominalExp = new Double[b.length];
						int[] dpd = new int[b.length];
						Double[] rate = new Double[b.length];
						Double[] lateFee = new Double[b.length];
						Double[] pcf = new Double[b.length];
						Double[] icf = new Double[b.length];
						Double[] predictedEad = new Double[b.length];
						
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							String[] splitArray = b[i].split("\\|");														
							cashflowDate[index] = new SimpleDateFormat("yyyy-MM-dd").parse(splitArray[0]);
							maturityDate[index] = new SimpleDateFormat("yyyy-MM-dd").parse(splitArray[1]);
							fundedType[index] = splitArray[2];
							portfolioCode[index] = splitArray[3];
							productCode[index] = splitArray[4];
							stage[index] = Integer.parseInt(splitArray[5]);
							nominalExp[index] = Double.valueOf(splitArray[6]).doubleValue();
							dpd[index] = Integer.parseInt(splitArray[7]);
							rate[index] = Double.valueOf(splitArray[8]).doubleValue();
							lateFee[index] = Double.valueOf(splitArray[9]).doubleValue();
							pcf[index] = Double.valueOf(splitArray[10]).doubleValue();
							icf[index] = Double.valueOf(splitArray[11]).doubleValue();
							
							++index;
						}
					
					
						
						for(int i=0; i < b.length; i++) {
							if(i == 0 ) {
								if (fundedType[i].toUpperCase().equals("NON_FUNDED") || portfolioCode[i].toUpperCase().equals("INVESTMENT")) 
								{
								predictedEad[i]= nominalExp[i];
								} 
								 else if (stage[i]==1 || stage[i]==2) 
											{
											predictedEad[i]= nominalExp[i] + (nominalExp[i] * dpd[i] * rate[i] / 365);
											} else {
										predictedEad[i]=nominalExp[i] + lateFee[i] + (nominalExp[i] * dpd[i] * rate[i] / 365);
											}
							}
							else {
							
								if (fundedType[i].toUpperCase().equals("NON_FUNDED") || portfolioCode[i].toUpperCase().equals("INVESTMENT")) 
								{
								predictedEad[i]= nominalExp[i];
								} 
								else if (cashflowDate[i].equals(maturityDate[i])) 
									{
										predictedEad[i]=0.0;
									} 
								else if (productCode[i].toUpperCase().equals("CREDIT_CARD")|| productCode[i].toUpperCase().equals("CREDIT CARD")) 
									{
								predictedEad[i]=nominalExp[i] + (nominalExp[i] * 4 * rate[i] / 12);
									} else {
								predictedEad[i] = predictedEad[i-1] - pcf[i] + icf[i]  ;
								System.out.println("R = " + predictedEad[i]);
								
								}
						}
					}				
						
						Double result = predictedEad[index-1];
						
						return result;
					}
				}, DataTypes.DoubleType);
								
				// Register Correlation
				sqlContext.udf().register("correl", new UDF2<WrappedArray<Double>, WrappedArray<Double>, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<Double> x, WrappedArray<Double> y) throws Exception {
						
						Object b[] = new Object[x.length()];
						x.copyToArray(b);
						Object c[] = new Object[y.length()];
						y.copyToArray(c);

						double[] xArray = new double[x.length()];
						double[] yArray = new double[y.length()];
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							System.out.println(dub);
							xArray[i] = dub;
							
						}
						
						for (int i = 0; i < c.length; i++) {
							Object object = c[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							System.out.println(dub);
							yArray[i] = dub;
							
						}
						
						PearsonsCorrelation pc = new PearsonsCorrelation();
						return pc.correlation(xArray, yArray);
						
					}
				}, DataTypes.DoubleType);						
								
				// Second Worst score logic Mizuho Basel			
				sqlContext.udf().register("secondWorstScore", new UDF3<Long, Long, Long, Long>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long value1, Long value2, Long value3) throws Exception {
						Long result = null;
						if (value1 == null && value2 == null && value3 == null) {
							result = null;
						} else if (value1 != null && value2 == null && value3 == null) {
							result = value1;
						} else if (value1 == null && value2 != null && value3 == null) {
							result = value2;
						} else if (value1 == null && value2 == null && value3 != null) {
							result = value3;
						} else if (value1 == null && value2 != null && value3 != null) {
							long[] maxArray = new long[2];
							// BigDecimal b;
							maxArray[0] = value2.longValue();
							maxArray[1] = value3.longValue();
							result = NumberUtils.max(maxArray);
						} else if (value1 != null && value2 != null && value3 == null) {
							long[] maxArray = new long[2];
							// BigDecimal b;
							maxArray[0] = value1.longValue();
							maxArray[1] = value2.longValue();
							result = NumberUtils.max(maxArray);
						} else if (value1 != null && value2 == null && value3 != null) {
							long[] maxArray = new long[2];
							// BigDecimal b;
							maxArray[0] = value1.longValue();
							maxArray[1] = value3.longValue();
							result = NumberUtils.max(maxArray);
						} else if (value1 != null && value2 != null && value3 != null) {
							long[] maxArray = new long[3];
							// BigDecimal b;
							maxArray[0] = value1.longValue();
							maxArray[1] = value2.longValue();
							maxArray[2] = value3.longValue();

							result = mostFrequent(maxArray, 3);
							if (result == 0) {
								result = getSecondLargest(maxArray, 3);
							}

						}

						return result;
					}

				}, DataTypes.LongType);
				
				// Register Regression Equation for forecasted_logit_dr (ADCB)
				sqlContext.udf().register("logit_dr", new UDF3<WrappedArray<Double>, Double, Double, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<Double> logit_dr_1, Double coeff, Double macro_factor) throws Exception {
						
						Object b[] = new Object[logit_dr_1.length()];
						logit_dr_1.copyToArray(b);
						
						double[] logit1 = new double[logit_dr_1.length()];
						double[] logit2 = new double[logit_dr_1.length()];
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							System.out.println(dub);
							logit1[i] = dub;
						}
						
						for (int i = 0; i < b.length; i++) {
							if(i == 0) {
								logit2[i] = 0;
							}
							else if(i == 1) {
								logit2[i] = logit1[i] + (coeff * macro_factor);
							}
							else {
								logit2[i] = logit1[i] + coeff * logit2[i-1];
							}
							index++;
						}
						
						return logit2[index-1];
						
					}
				}, DataTypes.DoubleType);
				
				// Register PiT PD transition matrix (ADCB)
				sqlContext.udf().register("transition_pit", new UDF5<WrappedArray<Double>, Double, Double, Integer, Integer, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<Double> ttc_input, Double lln_ctrl1, Double lln_ctrl2, Integer rating_num, Integer prediction_num) throws Exception {
						
						Object b[] = new Object[ttc_input.length()];
						ttc_input.copyToArray(b);
						
						int n = ttc_input.length();
						int p = prediction_num;
						int r = rating_num;
						
						double[] ttc1 = new double[ttc_input.length()];
						double[][] ttc = new double[p][r];
						double[][] pit = new double[p][r];
						
						double cum_ttc = 0;
						double cum_pit = 0;
						int r1 = 0;
						
						NormalDistribution nd = new NormalDistribution();
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							Object object = b[i];
							String string = object.toString();
							double dub = Double.valueOf(string).doubleValue();
							ttc1[i] = dub;
						}
						
						int count = 0;
						
						for (int i = 0; i < p; i++) {
							for (int j = 0; j < r; j++) {
								ttc[i][j] = ttc1[count];
								count++;
								if (count == n) {
									break;
								}
							}
						}
						
						for (int i = 0; i < p; i++) {
							cum_pit = 0;
							r1 = 0;
							for(int j = 0; j < r; j++ ) {
							
								cum_ttc = 0;
							
								if(i == 0) {
									pit[i][j] = ttc[i][j];
								}
								else {
									for (int k= 0; k <= j; k++) {
										cum_ttc = cum_ttc + ttc[i][k];
									}
									
									if (j == 0) {
										if (cum_ttc >= 1) {
											pit[i][j] = nd.cumulativeProbability((50 - lln_ctrl1) / lln_ctrl2);
										}
										else {
											pit[i][j] = nd.cumulativeProbability((nd.inverseCumulativeProbability(cum_ttc) - lln_ctrl1) / lln_ctrl2);
										}
										
									}
									else {
										cum_pit = cum_pit + pit[i][j-1];
										
										if(cum_ttc >=1) {
											pit[i][j] = nd.cumulativeProbability((50 - lln_ctrl1) / lln_ctrl2) - cum_pit;
										}
										else {
											pit[i][j] = nd.cumulativeProbability((nd.inverseCumulativeProbability(cum_ttc) - lln_ctrl1) / lln_ctrl2) - cum_pit;
										}
										
									}
								}
								
								index++;
								r1++;
								
								if(index == n) {
									break;
								}
							}
						}
						
						return pit[p-1][r1-1];
						
					}
				}, DataTypes.DoubleType);
				
				// exposure_at_default Calculation (ADCB EAD)
				sqlContext.udf().register("expDef", new UDF3<WrappedArray<String>, WrappedArray<Double>, Double, Double >() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wCashflow, WrappedArray<Double> cashflow, Double interest_rate ) throws Exception {
						
						String b[] = new String[wCashflow.length()];
						wCashflow.copyToArray(b);
						Double c[] = new Double[cashflow.length()];
						cashflow.copyToArray(c);
							
						double rate = interest_rate;
						double[] value = new double[cashflow.length()];
						Date[] dates = new Date[b.length];
						Double[] expDef = new Double[b.length];
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							
							Object object = b[i];
							String string = object.toString();
							dates[index] = new SimpleDateFormat("yyyy-MM-dd").parse(string);
							Object objectc = c[i];
							String stringc = objectc.toString();
							double dub = Double.valueOf(stringc).doubleValue();
							value[index] = dub;

							
							++index;
						}
						
						
						for(int i=0; i < b.length; i++) {
							if(i == 0 ) {
								expDef[i] = -value[i];
							}
							else {
								expDef[i] = expDef[i-1] - value[i] + (expDef[i-1]*rate/12) ;
								System.out.println("expDef["+i+"-1] = "+expDef[i-1]);

								System.out.println("cashflow ="+value);

								System.out.println("rate ="+rate);
								System.out.println("expDef["+i+"] = "+expDef[i]);
								}
						}
											
						System.out.println("Length ="+b.length);

						Double result = expDef[index-1];

						return result;
					}
				}, DataTypes.DoubleType);		
				
				// Opening EAD Calculation (NTB Predicted_EAD)
				sqlContext.udf().register("ntbEad", new UDF1<WrappedArray<String>, Double >() {

					/** */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(WrappedArray<String> wCashflow ) throws Exception {
						
						String b[] = new String[wCashflow.length()];
						wCashflow.copyToArray(b);
						
						Date[]  cashflowDate = new Date[b.length];
						Double[] install_amount = new Double[b.length];
						Double[] principal_rcy = new Double[b.length];
						Double[] accrued_rcy = new Double[b.length];
						Double[] interest_suspense = new Double[b.length];
						Double[] eir = new Double[b.length];
						int[] counter = new int[b.length];
						Double[] ntbEad = new Double[b.length];
						
						
						int index = 0;
						
						for (int i = 0; i < b.length; i++) {
							String[] splitArray = b[i].split("\\|");														
							cashflowDate[index] = new SimpleDateFormat("yyyy-MM-dd").parse(splitArray[0]);
							install_amount[index] = Double.valueOf(splitArray[1]).doubleValue();
							principal_rcy[index] = Double.valueOf(splitArray[2]).doubleValue();
							accrued_rcy[index] = Double.valueOf(splitArray[3]).doubleValue();
							interest_suspense[index] = Double.valueOf(splitArray[4]).doubleValue();
							eir[index] = Double.valueOf(splitArray[5]).doubleValue();
							counter[index] = Integer.parseInt(splitArray[6]);
							
							++index;
						}
					
					
						
						for(int i=0; i < b.length; i++) {
							if(i == 0 ) {
								ntbEad[i]= principal_rcy[i]+accrued_rcy[i]-interest_suspense[i];
							}
							
							else if(i == 1 ) {
								ntbEad[i]= principal_rcy[i]+principal_rcy[i] *(eir[i]/12) - install_amount[i];
							}
							
							else {
							
								ntbEad[i] = ntbEad[i-1] + ntbEad[i-1] *(eir[i]/12) - install_amount[i]  ;
								System.out.println("R = " + ntbEad[i]);
								
								}
						}
									
						
						Double result = ntbEad[index-1];
						
						return result;
					}
				}, DataTypes.DoubleType);			
				
				
				// Interpolation, MS Excel's forecast function
				sqlContext.udf().register("forecast", new UDF3<Double, WrappedArray<Double>, WrappedArray<Double>, Double>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 5509600037251745281L;

					
					public Double call(Double tenor, WrappedArray<Double> rateArray, WrappedArray<Double> tenorArray)
							throws Exception {						
						
						Double arrY[] = new Double[rateArray.length()];
						rateArray.copyToArray(arrY);
						
						Double arrX[] = new Double[tenorArray.length()];
						tenorArray.copyToArray(arrX);
						
						// Calculate average of Y
						final double averageY;						
						double sum = 0.0d;
						for (double value : arrY) {
				            sum += value;
				        }
						averageY = sum / arrY.length;
						
			            // Calculate average of X
						final double averageX;						
						sum = 0.0d;
						for (double value : arrX) {
				            sum += value;
				        }
						averageX = sum / arrX.length;
			            
			            double bnum = 0;
			            double bdem = 0;
			            final int len = arrY.length;
			            for (int i = 0; i < len; i++) {
			                double diff0 = arrX[i] - averageX;
			                bnum += diff0 * (arrY[i] - averageY);
			                bdem += Math.pow(diff0, 2);
			            }
			            if (bdem == 0) {
			                throw new RuntimeException("Divide by Zero error while calculating for: " + tenor);
			            }

			            final double b = bnum / bdem;
			            final double a = averageY - (b * averageX);
			            final double res = a + (b * tenor);
			            return res;
					}								
					
				}, DataTypes.DoubleType);
				
				//Monthly Cumulative PD(ADCB POC)
				sqlContext.udf().register("MonCumPD", new UDF1<WrappedArray<String>, Double >() {

                    /** */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Double call(WrappedArray<String> wCashflow ) throws Exception {
                        
                        String b[] = new String[wCashflow.length()];
                        wCashflow.copyToArray(b);
                        
                        Double[] monthly_survival = new Double[b.length];
                        int[] pred_freq = new int[b.length];
                        Double[] MonCumPD = new Double[b.length];
                        
                        
                        int index = 0;
                        
                        for (int i = 0; i < b.length; i++) {
                            String[] splitArray = b[i].split("\\|");                                                        
                        /*  cashflowDate[index] = new SimpleDateFormat("yyyy-MM-dd").parse(splitArray[0]);*/
                            monthly_survival[index] = Double.valueOf(splitArray[0]).doubleValue();
                            pred_freq[index] = Integer.parseInt(splitArray[1]);
                            
                            ++index;
                        }
                    
                    
                        
                        for(int i=0; i < b.length; i++) {
                            if(i == 0 ) {
                                MonCumPD[i]= 0.0;
                            }
                            
                            
                            
                            else {
                                 if(pred_freq[i] == 1 ) {
                                    MonCumPD[i]= MonCumPD[i-1] + monthly_survival[i] ;
                                }
                                
                                 else {
                                MonCumPD[i] = MonCumPD[i-1] + (monthly_survival[i]  - monthly_survival[i-1] );
                                System.out.println("R = " + MonCumPD[i]);
                                 }
                                }
                        }
                                    
                        
                        Double result = MonCumPD[index-1];
                        
                        return result;
                    }
                }, DataTypes.DoubleType);	
				
				// Interpolation, MS Excel's forecast for Deposits function for Mizuho FTP 
				sqlContext.udf().register("forecastDeposit", new UDF3<Double, WrappedArray<Double>, WrappedArray<Double>, Double>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 5509600037251745281L;

					
					public Double call(Double tenor, WrappedArray<Double> rateArray, WrappedArray<Double> tenorArray)
							throws Exception {						
						
						Double arrY[] = new Double[rateArray.length()];
						rateArray.copyToArray(arrY);
						
						Double arrX[] = new Double[tenorArray.length()];
						tenorArray.copyToArray(arrX);
						
						// Calculate average of Y
						final double averageY;						
						double sum = 0.0d;
						for (double value : arrY) {
				            sum += value;
				        }
						averageY = sum / arrY.length;
						
			            // Calculate average of X
						final double averageX;						
						sum = 0.0d;
						for (double value : arrX) {
				            sum += value;
				        }
						averageX = sum / arrX.length;
			            
			            double bnum = 0;
			            double bdem = 0;
			            final int len = arrY.length;
			            for (int i = 0; i < len; i++) {
			                double diff0 = arrX[i] - averageX;
			                bnum += diff0 * (arrY[i] - averageY);
			                bdem += Math.pow(diff0, 2);
			            }
			            if (bdem == 0) {
			                throw new RuntimeException("Divide by Zero error while calculating for: " + tenor);
			            }

			            final double b = bnum / bdem;
			            final double a = averageY - (b * averageX);
			            final double res = a + (b * tenor);
			          //added logic for mizuho ftp forecast deposit  			           
			            final double  rate=   res + 0.0025;
			            final double  trimmedRate = Math.floor(rate * 100) / 100;
			            double finalResult=0.0;    

			            if (Math.abs((rate - trimmedRate)) >= 0.005)
			                	finalResult = trimmedRate + 0.005;
			                else
			                	finalResult = trimmedRate;
		            
			            return finalResult;
					}								
					
				}, DataTypes.DoubleType);		
				
	}

	public long getSecondLargest(long[] a, int total) {
		long temp;
		for (int i = 0; i < total; i++) {
			for (int j = i + 1; j < total; j++) {
				if (a[i] > a[j]) {
					temp = a[i];
					a[i] = a[j];
					a[j] = temp;
				}
			}
		}
		return a[total - 2];
	}

	public long mostFrequent(long[] arr, int n) {
		long maxcount = 0;
		long element_having_max_freq = 0;
		for (int i = 0; i < n; i++) {
			long count = 0;
			for (int j = 0; j < n; j++) {
				if (arr[i] == arr[j]) {
					count++;
				}
			}

			if (count > maxcount) {
				maxcount = count;
				element_having_max_freq = arr[i];
			}
		}
		if (maxcount > 1) {
			return element_having_max_freq;
		} else {
			return 0;
		}

	}
}
