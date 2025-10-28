/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.time.Duration;

/**
 * Unit tests for TimeshiftPlanNode.
 */
public class TimeshiftPlanNodeTests extends BasePlanNodeTests {

    public void testTimeshiftPlanNodeCreation() {
        TimeshiftPlanNode node = new TimeshiftPlanNode(1, "1d");

        assertEquals(1, node.getId());
        assertEquals(Duration.ofDays(1), node.getDuration());
        assertEquals("TIMESHIFT(1d)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testTimeshiftPlanNodeWithHours() {
        TimeshiftPlanNode node = new TimeshiftPlanNode(1, "6h");

        assertEquals(Duration.ofHours(6), node.getDuration());
        assertEquals("TIMESHIFT(6h)", node.getExplainName());
    }

    public void testTimeshiftPlanNodeWithMinutes() {
        TimeshiftPlanNode node = new TimeshiftPlanNode(1, "30m");

        assertEquals(Duration.ofMinutes(30), node.getDuration());
        assertEquals("TIMESHIFT(30m)", node.getExplainName());
    }

    public void testTimeshiftPlanNodeVisitorAccept() {
        TimeshiftPlanNode node = new TimeshiftPlanNode(1, "2h");
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit TimeshiftPlanNode", result);
    }

    public void testTimeshiftPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("timeshift");
        functionNode.addChildNode(new ValueNode("2h"));

        TimeshiftPlanNode node = TimeshiftPlanNode.of(functionNode);

        assertEquals(Duration.ofHours(2), node.getDuration());
    }

    public void testTimeshiftPlanNodeFactoryMethodWithDays() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("timeshift");
        functionNode.addChildNode(new ValueNode("7d"));

        TimeshiftPlanNode node = TimeshiftPlanNode.of(functionNode);

        assertEquals(Duration.ofDays(7), node.getDuration());
    }

    public void testTimeshiftPlanNodeFactoryMethodWithSeconds() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("timeshift");
        functionNode.addChildNode(new ValueNode("120s"));

        TimeshiftPlanNode node = TimeshiftPlanNode.of(functionNode);

        assertEquals(Duration.ofSeconds(120), node.getDuration());
    }

    public void testTimeshiftPlanNodeHandlesNegativeDuration() {
        TimeshiftPlanNode node = new TimeshiftPlanNode(1, "-120s");

        assertEquals("Negative duration should be converted to positive", Duration.ofSeconds(120), node.getDuration());
    }

    public void testTimeshiftPlanNodeConvertsNegativeDurationToAbsolute() {
        // Test various negative durations
        TimeshiftPlanNode node1 = new TimeshiftPlanNode(1, "-2h");
        assertEquals(Duration.ofHours(2), node1.getDuration());

        TimeshiftPlanNode node2 = new TimeshiftPlanNode(2, "-30m");
        assertEquals(Duration.ofMinutes(30), node2.getDuration());

        TimeshiftPlanNode node3 = new TimeshiftPlanNode(3, "-1d");
        assertEquals(Duration.ofDays(1), node3.getDuration());
    }

    public void testTimeshiftPlanNodePreservesPositiveDuration() {
        // Test that positive durations remain unchanged
        TimeshiftPlanNode node1 = new TimeshiftPlanNode(1, "2h");
        assertEquals(Duration.ofHours(2), node1.getDuration());

        TimeshiftPlanNode node2 = new TimeshiftPlanNode(2, "30m");
        assertEquals(Duration.ofMinutes(30), node2.getDuration());
    }

    public void testTimeshiftPlanNodeFactoryMethodThrowsOnNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("timeshift");

        expectThrows(IllegalArgumentException.class, () -> TimeshiftPlanNode.of(functionNode));
    }

    public void testTimeshiftPlanNodeFactoryMethodThrowsOnMultipleArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("timeshift");
        functionNode.addChildNode(new ValueNode("1h"));
        functionNode.addChildNode(new ValueNode("2h"));

        expectThrows(IllegalArgumentException.class, () -> TimeshiftPlanNode.of(functionNode));
    }

    public void testTimeshiftPlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("timeshift");
        functionNode.addChildNode(new FunctionNode()); // not a value node

        expectThrows(IllegalArgumentException.class, () -> TimeshiftPlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(TimeshiftPlanNode planNode) {
            return "visit TimeshiftPlanNode";
        }
    }
}
