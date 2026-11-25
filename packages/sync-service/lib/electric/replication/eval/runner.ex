defmodule Electric.Replication.Eval.Runner do
  require Logger
  alias Electric.Utils
  alias Electric.Replication.Eval.Expr
  alias Electric.Replication.Eval.Env
  alias Electric.Replication.Eval.SublinkKey
  require SublinkKey
  alias Electric.Replication.Eval.Parser.{Const, Func, Ref, RowExpr, Array}

  defmodule Unreadable do
    @moduledoc """
    Stands in for a column value this environment cannot parse.

    Postgres renders values that `Env.parse_const/3` refuses -- `infinity` and years past 9999 for
    the date types, `NaN` and the infinities for the numeric ones -- on columns whose *type* the
    same environment calls castable. Halting the whole row on one of those denied an answer to
    every predicate naming that column, including the ones Postgres answers by short-circuiting
    past it, and the consumer reads a denied answer as a non-match.

    Nullity is the one thing knowable about such a value without parsing it, so the null tests are
    allowed to see this and everything else must refuse it. A bare marker would not be safe on its
    own: Erlang orders an atom below a map, so a sentinel reaching `>` against a `DateTime` would
    answer `false` rather than raise. The refusal is enforced in `apply_func/2`, not by the term.
    """
    defstruct [:type]
  end

  @doc """
  Generate a ref values object based on the record and a given table name

  ## Examples

      iex> used_refs = %{["id"] => :int8, ["created_at"] => :timestamp}
      iex> record_to_ref_values(used_refs, %{"id" => "80", "created_at" => "2020-01-01T11:00:00Z"})
      %{
        ["id"] => 80,
        ["created_at"] => ~N[2020-01-01 11:00:00]
      }

  A value this environment cannot parse becomes an `Unreadable` marker rather than failing the
  whole row, so a predicate whose answer does not depend on that column still gets one.
  """
  @spec record_to_ref_values(Expr.used_refs(), map(), Env.t()) :: {:ok, map()}
  def record_to_ref_values(used_refs, record, env \\ Env.new()) do
    used_refs
    # Keep only used refs that are pointing to current table
    |> Enum.filter(&match?({[_], _}, &1))
    |> Enum.reduce_while({:ok, %{}}, fn {[key] = path, type}, {:ok, acc} ->
      value = record[key]
      acc = keep_sublink_text(acc, path, type, value)

      case Env.parse_const(env, value, type) do
        {:ok, value} ->
          {:cont, {:ok, Map.put(acc, path, value)}}

        :error ->
          # The value is row data, so the warning names only the column and the type.
          Logger.warning(
            "Could not parse a value as #{inspect(type)} while casting #{inspect(key)}"
          )

          # Its text is still the key an `IN (SELECT ...)` compares it by (see `SublinkKey`).
          acc = if is_binary(value), do: Map.put(acc, {:sublink_text, path}, value), else: acc
          {:cont, {:ok, Map.put(acc, path, %Unreadable{type: type})}}
      end
    end)
  end

  # A `numeric` parses to a float, which is not the key a subquery compares it by (see
  # `SublinkKey`), so its text is kept too, for the left side of an `IN (SELECT ...)`. So is the
  # text of a `numeric[]`.
  defp keep_sublink_text(acc, path, type, value)
       when SublinkKey.exact_type?(type) and is_binary(value),
       do: Map.put(acc, {:sublink_text, path}, value)

  defp keep_sublink_text(acc, _path, _type, _value), do: acc

  @doc """
  Evaluate the left side of an `IN (SELECT ...)`, a column or a row of columns, into the value the
  subquery's view holds for it (see `SublinkKey`).
  """
  @spec execute_sublink_lhs(Expr.t(), map()) :: {:ok, term()} | {:error, term()}
  def execute_sublink_lhs(%Expr{} = testexpr, ref_values) do
    case sublink_lhs(testexpr.eval, %{refs: ref_values, subquery_member?: nil}) do
      {:ok, %Unreadable{} = unreadable} -> {:error, unreadable}
      result -> result
    end
  catch
    {:could_not_compute, func} -> {:error, func}
  end

  def execute_for_record(expr, record, extra_refs \\ %{}) do
    with {:ok, ref_values} <- record_to_ref_values(expr.used_refs, record),
         {:ok, evaluated} <- execute(expr, Map.merge(ref_values, extra_refs)) do
      {:ok, evaluated}
    end
  end

  @doc """
  Run a PG function parsed by `Electric.Replication.Eval.Parser` based on the inputs
  """
  @spec execute(Expr.t(), map(), keyword()) :: {:ok, term()} | {:error, {%Func{}, [term()]}}
  def execute(%Expr{} = tree, ref_values, opts \\ []) do
    ctx = %{refs: ref_values, subquery_member?: Keyword.get(opts, :subquery_member?)}

    case execute_node(tree.eval, ctx) do
      # A bare unparseable column as the whole expression: nothing narrowed it to a nullity test,
      # so there is no answer to give.
      {:ok, %Unreadable{} = unreadable} -> {:error, unreadable}
      result -> result
    end
  catch
    {:could_not_compute, func} -> {:error, func}
  end

  defp execute_node(%Const{value: value}, _ctx), do: {:ok, value}

  defp execute_node(
         %Ref{path: ["$sublink", _] = path},
         %{refs: refs, subquery_member?: subquery_member?}
       )
       when is_function(subquery_member?, 2) do
    {:ok, Map.get(refs, path, {:subquery_ref, path})}
  end

  defp execute_node(%Ref{path: path}, %{refs: refs}), do: {:ok, Map.fetch!(refs, path)}

  # An `ARRAY[...]` of arrays builds an array of them; the elements of any other are its values.
  defp execute_node(%Array{elements: [%{type: {:array, _}} | _] = elements} = array, ctx) do
    with {:ok, values} <- Utils.map_while_ok(elements, &execute_node(&1, ctx)) do
      {:ok, construct_array(array, values)}
    end
  end

  defp execute_node(%Array{elements: elements}, ctx) do
    Utils.map_while_ok(elements, &execute_node(&1, ctx))
  end

  defp execute_node(%RowExpr{elements: elements}, ctx) do
    with {:ok, elements} <- Utils.map_while_ok(elements, &execute_node(&1, ctx)) do
      {:ok, List.to_tuple(elements)}
    end
  end

  defp execute_node(
         %Func{name: "coalesce", variadic_arg: 0, args: [%Array{elements: elements}]},
         ctx
       ) do
    execute_coalesce(elements, ctx)
  end

  defp execute_node(
         %Func{name: "sublink_membership_check"} = func,
         %{subquery_member?: subquery_member?} = ctx
       )
       when is_function(subquery_member?, 2) do
    with {:ok, [value, {:subquery_ref, path}]} <- sublink_args(func.args, ctx) do
      # This clause applies the function itself rather than going through `apply_func/2`, so it
      # needs the same refusal: a set lookup answers `false` for an unreadable value instead of
      # raising, and that is a guess, not an answer.
      if unreadable_arg?([value]) do
        throw({:could_not_compute, %{func | args: [value, {:subquery_ref, path}]}})
      end

      {:ok,
       try do
         subquery_member?.(path, value)
       rescue
         _ -> throw({:could_not_compute, %{func | args: [value, {:subquery_ref, path}]}})
       end}
    end
  end

  defp execute_node(%Func{name: "sublink_membership_check"} = func, ctx) do
    with {:ok, args} <- sublink_args(func.args, ctx) do
      apply_func(func, args)
    end
  end

  defp execute_node(%Func{args: args, variadic_arg: position} = func, ctx) do
    with {:ok, args} <- execute_args(args, position, ctx) do
      apply_func(func, args)
    end
  end

  defp execute_args(args, nil, ctx), do: Utils.map_while_ok(args, &execute_node(&1, ctx))

  # The parser puts a variadic function's last arguments in an `Array` node, which holds them as a
  # list and is not an `ARRAY[...]` the row builds.
  defp execute_args(args, position, ctx) do
    case Enum.split(args, position) do
      {fixed, [%Array{elements: variadic}]} ->
        with {:ok, fixed} <- execute_args(fixed, nil, ctx),
             {:ok, variadic} <- execute_args(variadic, nil, ctx) do
          {:ok, fixed ++ [variadic]}
        end

      _ ->
        execute_args(args, nil, ctx)
    end
  end

  defp sublink_args([testexpr, sublink], ctx) do
    with {:ok, value} <- sublink_lhs(testexpr, ctx),
         {:ok, view} <- execute_node(sublink, ctx) do
      {:ok, [value, view]}
    end
  end

  defp sublink_lhs(%RowExpr{elements: elements}, ctx) do
    with {:ok, elements} <- Utils.map_while_ok(elements, &sublink_lhs(&1, ctx)) do
      {:ok, List.to_tuple(elements)}
    end
  end

  defp sublink_lhs(%Ref{path: path, type: type} = ref, %{refs: refs} = ctx)
       when SublinkKey.exact_type?(type) do
    with {:ok, text} <- Map.fetch(refs, {:sublink_text, path}),
         {:ok, key} <- SublinkKey.parse(text, type) do
      {:ok, key}
    else
      :error -> execute_node(ref, ctx)
    end
  end

  defp sublink_lhs(%Ref{path: path, type: type} = ref, %{refs: refs} = ctx) do
    case execute_node(ref, ctx) do
      {:ok, %Unreadable{}} = unreadable ->
        case Map.fetch(refs, {:sublink_text, path}) do
          {:ok, text} -> with :error <- SublinkKey.parse(text, type), do: unreadable
          :error -> unreadable
        end

      {:ok, value} ->
        {:ok, SublinkKey.canonical(type, value)}
    end
  end

  defp sublink_lhs(node, ctx) do
    with {:ok, value} <- execute_node(node, ctx) do
      {:ok, SublinkKey.canonical(node.type, value)}
    end
  end

  # Postgres refuses to build an array of arrays whose dimensions or lower bounds differ, and it
  # leaves undefined whether it evaluates an operand whose answer it does not need, so a clause that
  # builds one, even `a[0] = 1 OR ARRAY[a, NULL] IS NULL`, has no answer for the row.
  defp construct_array(array, values) do
    PgInterop.Array.construct(values)
  rescue
    _ -> throw({:could_not_compute, array})
  end

  defp execute_coalesce([], _ctx), do: {:ok, nil}

  defp execute_coalesce([arg | rest], ctx) do
    case execute_node(arg, ctx) do
      {:ok, nil} -> execute_coalesce(rest, ctx)
      {:ok, value} -> {:ok, value}
    end
  end

  # `IS NULL` / `IS NOT NULL` compile to `Kernel.is_nil/1` over the ref value, and an unparseable
  # value is not nil -- which is the answer Postgres gives, without needing the value read. Matching
  # on the name follows `SqlGenerator`, which discriminates the same tests the same way.
  @nullity_only_funcs ["IS_NULL", "IS_NOT_NULL"]

  defp apply_func(%Func{name: name} = func, args) when name in @nullity_only_funcs,
    do: {:ok, try_apply(func, args)}

  defp apply_func(%Func{} = func, args) do
    if unreadable_arg?(args) do
      throw({:could_not_compute, func})
    end

    apply_readable_func(func, args)
  end

  # A marker reaches an argument inside whatever container the ref sat in: a variadic position and
  # an array literal hold theirs in a list, and `RowExpr` -- the left side of a composite
  # `IN (SELECT ...)` -- builds a tuple. Descending both, to any depth, is what keeps the refusal a
  # property of the value rather than of the shapes of container it was written against.
  defp unreadable_arg?(args), do: Enum.any?(args, &unreadable?/1)

  defp unreadable?(%Unreadable{}), do: true
  defp unreadable?(value) when is_list(value), do: Enum.any?(value, &unreadable?/1)

  defp unreadable?(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.any?(&unreadable?/1)

  defp unreadable?(_), do: false

  defp apply_readable_func(%Func{strict?: false} = func, args) do
    # For a non-strict function, we don't care about nil values in the arguments
    {:ok, try_apply(func, args)}
  end

  defp apply_readable_func(%Func{strict?: true, variadic_arg: vararg_position} = func, args) do
    has_nils? =
      case vararg_position do
        nil -> Enum.any?(args, &is_nil/1)
        pos -> Enum.any?(Enum.at(args, pos), &is_nil/1) or Enum.any?(args, &is_nil/1)
      end

    if has_nils? do
      {:ok, nil}
    else
      {:ok, try_apply(func, args)}
    end
  end

  defp try_apply(func, args) do
    Func.apply_to(func, args)
  rescue
    _ ->
      # Anything could have gone wrong here
      throw({:could_not_compute, %{func | args: args}})
  end
end
