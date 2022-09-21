// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

public class OptimizerConfig {
    public enum OptimizerAlgorithm {
        RULE_BASED,
        COST_BASED
    }
    private OptimizerAlgorithm optimizerAlgorithm;
    private boolean enableMvRewrite;

    private static OptimizerConfig DEFAULT_CONFIG = new OptimizerConfig();

    public static OptimizerConfig defaultConfig() {
        return DEFAULT_CONFIG;
    }

    public OptimizerConfig() {
        this.optimizerAlgorithm = OptimizerAlgorithm.COST_BASED;
        this.enableMvRewrite = true;
    }

    public OptimizerConfig(OptimizerAlgorithm optimizerAlgorithm, boolean enableMvRewrite) {
        this.optimizerAlgorithm = optimizerAlgorithm;
        this.enableMvRewrite = enableMvRewrite;
    }

    public boolean isRuleBased() {
        return optimizerAlgorithm.equals(OptimizerAlgorithm.RULE_BASED);
    }

    public boolean isEnableMvRewrite() {
        return enableMvRewrite;
    }
}
