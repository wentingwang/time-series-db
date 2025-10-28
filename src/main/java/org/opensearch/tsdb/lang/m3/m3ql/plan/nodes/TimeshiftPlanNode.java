/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.M3Duration;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.time.Duration;
import java.util.Locale;

/**
 * TimeshiftPlanNode represents a node in the M3QL plan that applies a time shift to a time series.
 */
public class TimeshiftPlanNode extends M3PlanNode {

    private final String duration;

    /**
     * Default constructor for TimeshiftPlanNode.
     *
     * @param id node id
     * @param duration the duration to shift the time series (e.g., "1h" for 1 hour)
     */
    public TimeshiftPlanNode(int id, String duration) {
        super(id);
        this.duration = duration;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.getDefault(), "TIMESHIFT(%s)", duration);
    }

    /**
     * Returns the absolute duration for the time shift.
     * Negative durations are converted to positive values.
     * @return Duration object representing the absolute time shift period
     */
    public Duration getDuration() {
        Duration d = M3Duration.valueOf(duration);
        return d.isNegative() ? d.negated() : d;
    }

    /**
     * Factory method to create a TimeshiftPlanNode from a FunctionNode.
     * @param functionNode the function node representing the TIMESHIFT function
     * @return a new TimeshiftPlanNode instance
     */
    public static TimeshiftPlanNode of(FunctionNode functionNode) {
        if (functionNode.getChildren().size() != 1) {
            throw new IllegalArgumentException("Timeshift function expects exactly one argument");
        }
        if (!(functionNode.getChildren().getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Timeshift expects a value node as the first argument");
        }

        String duration = valueNode.getValue();
        return new TimeshiftPlanNode(M3PlannerContext.generateId(), duration);
    }
}
