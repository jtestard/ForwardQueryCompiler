package edu.ucsd.forward.query.logical;

import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Represents a placeholder used in unfinished operator trees.
 */
public class Placeholder extends AbstractOperator {
    public static final Object DEFAULT_TAG = new Object();

    private final Object tag;

    public Placeholder(Object tag) {
        this.tag = checkNotNull(tag);
    }

    public Placeholder() {
        this(DEFAULT_TAG);
    }

    public Object getTag() {
        return tag;
    }

    /**
     * Removes the first occurrence of this placeholder from its parent's children list, and returns its index in the children list
     * before removal.
     */
    public int removeFromParent() {
        checkNotNull(getParent());
        int i = 0;
        List<Operator> children = getParent().getChildren();
        for (Operator child : children) {
            if (child == this) {
                break;
            }
            ++i;
        }
        checkState(i < children.size(), "Cannot find placeholder in its parent's children list");
        getParent().removeChild(this);
        return i;
    }

    @Override
    public void updateOutputInfo() throws QueryCompilationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Operator accept(OperatorVisitor visitor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Operator copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Placeholder copyWithoutType() {
        return new Placeholder(tag);
    }

    @Override
    public List<Parameter> getFreeParametersUsed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Parameter> getBoundParametersUsed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Variable> getVariablesUsed() {
        throw new UnsupportedOperationException();
    }
}
