package com.fs.starfarer.api.campaign.impl.items;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import java.awt.Color;

import org.json.JSONException;

import com.fs.starfarer.api.Global;
import com.fs.starfarer.api.campaign.CargoStackAPI;
import com.fs.starfarer.api.campaign.CargoTransferHandlerAPI;
import com.fs.starfarer.api.campaign.SpecialItemPlugin;
import com.fs.starfarer.api.campaign.SpecialItemSpecAPI;
import com.fs.starfarer.api.campaign.SubmarketPlugin.TransferAction;
import com.fs.starfarer.api.campaign.econ.MarketAPI;
import com.fs.starfarer.api.campaign.econ.SubmarketAPI;
import com.fs.starfarer.api.combat.ShipHullSpecAPI;
import com.fs.starfarer.api.impl.campaign.ids.Tags;
import com.fs.starfarer.api.loading.FighterWingSpecAPI;
import com.fs.starfarer.api.loading.WeaponSpecAPI;
import com.fs.starfarer.api.ui.LabelAPI;
import com.fs.starfarer.api.ui.TooltipMakerAPI;
import com.fs.starfarer.api.util.Highlights;
import com.fs.starfarer.api.util.Misc;

public class BaseSpecialItemPlugin implements SpecialItemPlugin {

	protected SpecialItemSpecAPI spec;
	protected CargoStackAPI stack;
	protected String itemId;
	
	
	public String getId() {
		return itemId;
	}
	public void setId(String id) {
		this.itemId = id;
		spec = Global.getSettings().getSpecialItemSpec(id);
	}
	public void init(CargoStackAPI stack) {
		this.stack = stack;
	}

	@Override
	public void performRightClickAction(RightClickActionHelper helper) {
		performRightClickAction();
	}
	public void performRightClickAction() {
		
	}
	
	public boolean hasRightClickAction() {
		return false;
	}

	public boolean shouldRemoveOnRightClickAction() {
		return true;
	}
	
	
	public void createTooltip(TooltipMakerAPI tooltip, boolean expanded, CargoTransferHandlerAPI transferHandler, Object stackSource) {
		boolean useGray = true;
		if (getClass() == BaseSpecialItemPlugin.class || Global.CODEX_TOOLTIP_MODE) {
			useGray = false;
		}
		createTooltip(tooltip, expanded, transferHandler, stackSource, useGray);
	}
	public void createTooltip(TooltipMakerAPI tooltip, boolean expanded, CargoTransferHandlerAPI transferHandler, Object stackSource, boolean useGray) {
		float opad = 10f;
		
		if (!Global.CODEX_TOOLTIP_MODE) {
			tooltip.addTitle(getName());
		} else {
			tooltip.addSpacer(-opad);
		}
		
		String design = getDesignType();
		Misc.addDesignTypePara(tooltip, design, opad);
		
		if (Global.CODEX_TOOLTIP_MODE) {
			tooltip.setParaSmallInsignia();
		}
		if (!spec.getDesc().isEmpty()) {
			Color c = Misc.getTextColor();
			if (useGray) c = Misc.getGrayColor();
			tooltip.addPara(spec.getDesc(), c, opad);
		}
		
		if (getClass() == BaseSpecialItemPlugin.class) {
			addCostLabel(tooltip, opad, transferHandler, stackSource);
		}
	}

	public float getTooltipWidth() {
		return 450;
	}

	public boolean isTooltipExpandable() {
		return false;
	}

	public String getName() {
		return spec.getName();
	}

	public int getPrice(MarketAPI market, SubmarketAPI submarket) {
		if (spec != null) return (int) spec.getBasePrice();
		return 0;
	}

	protected float getItemPriceMult() {
		return Global.getSettings().getFloat("blueprintPriceOriginalItemMult");
	}
	
	public void render(float x, float y, float w, float h, float alphaMult, float glowMult, SpecialItemRendererAPI renderer) {
		
	}

	
	
	protected void addCostLabel(TooltipMakerAPI tooltip, float pad, CargoTransferHandlerAPI transferHandler, Object stackSource) {
		if (stack == null) return;
		
		//if (Global.CODEX_TOOLTIP_MODE) return;
		if (Global.CODEX_TOOLTIP_MODE) {
			int cost = (int) stack.getBaseValuePerUnit();
			tooltip.setParaFontDefault();
			tooltip.addPara("Base value: %s", pad, Misc.getGrayColor(), Misc.getHighlightColor(), Misc.getDGSCredits(cost));
			return;
			//tooltip.setParaSmallInsignia();
		}
		
		ItemCostLabelData data = getCostLabelData(stack, transferHandler, stackSource);
		
		LabelAPI label = tooltip.addPara(data.text, pad);
		if (data.highlights != null) {
			label.setHighlight(data.highlights.getText());
			label.setHighlightColors(data.highlights.getColors());
		}
	}
	
	
	public static class ItemCostLabelData {
		public String text;
		public Highlights highlights;
	}
	
	protected ItemCostLabelData getCostLabelData(CargoStackAPI stack, CargoTransferHandlerAPI transferHandler, Object stackSource) {
		String text = "";
		String highlight = null;
		Highlights highlights = null;
		Color highlightColor = Misc.getHighlightColor();
		TransferAction action = TransferAction.PLAYER_BUY;
		if (transferHandler != null && stackSource == transferHandler.getManifestOne()) {
			action = TransferAction.PLAYER_SELL;
		}
		
		
		if (action == TransferAction.PLAYER_SELL && stack.getSpecialItemSpecIfSpecial() != null) {
			SpecialItemSpecAPI spec = stack.getSpecialItemSpecIfSpecial();
			if (spec.hasTag(Tags.MISSION_ITEM)) {
				text = "Can not remove item";
				highlight = text;
				highlights = new Highlights();
				highlights.append(text, Misc.getNegativeHighlightColor());
				
				ItemCostLabelData data = new ItemCostLabelData();
				data.text = text;// + ".";
				data.highlights = highlights;
				
				return data;
			}
		}
		
		
		
		if (transferHandler != null && transferHandler.getSubmarketTradedWith() != null &&
				transferHandler.getSubmarketTradedWith().isIllegalOnSubmarket(stack, action)) {
			highlightColor = Misc.getNegativeHighlightColor();
			//text = "Illegal to trade on the " + transferHandler.getSubmarketTradedWith().getNameOneLine() + " here";
			text = transferHandler.getSubmarketTradedWith().getPlugin().getIllegalTransferText(stack, action);
			highlight = text;
			highlights = transferHandler.getSubmarketTradedWith().getPlugin().getIllegalTransferTextHighlights(stack, TransferAction.PLAYER_BUY);
		} else {
			if (stackSource != null && transferHandler != null && !transferHandler.isNoCost()) {
				if (stackSource == transferHandler.getManifestOne()) {
					int cost = (int)transferHandler.computeCurrentSingleItemSellCost(stack);
					//text = "Sells for: " + Misc.getWithDGS(cost) + " credits per unit";
					text = "Sells for: " + Misc.getDGSCredits(cost) + " per unit.";
					highlight = "" + Misc.getDGSCredits(cost);
				} else {
					int cost = (int)transferHandler.computeCurrentSingleItemBuyCost(stack);
					//text = "Price: " + Misc.getWithDGS(cost) + " credits per unit";
					text = "Price: " + Misc.getDGSCredits(cost) + " per unit.";
					highlight = "" + Misc.getDGSCredits(cost);
				}
			} else {
				int cost = (int) stack.getBaseValuePerUnit();
				//float mult = Global.getSettings().getFloat("nonEconItemSellPriceMult");
				//cost *= mult;
				//text = "Base value: " + Misc.getWithDGS(cost) + " credits per unit";
				text = "Base value: " + Misc.getDGSCredits(cost) + " per unit.";
				highlight = "" + Misc.getDGSCredits(cost);
			}
		}
		
		if (highlights == null) {
			highlights = new Highlights();
			highlights.setText(highlight);
			highlights.setColors(highlightColor);
		}
		
		ItemCostLabelData data = new ItemCostLabelData();
		data.text = text;// + ".";
		data.highlights = highlights;
		
		return data;
	}

	public String resolveDropParamsToSpecificItemData(String params, Random random) throws JSONException {
		return "";
	}
	
	
	
	
	protected static interface BlueprintLister {
		boolean isKnown(String id);
		String getNoun(int num);
		String getName(String id);
	}
	
	protected void addShipList(TooltipMakerAPI tooltip, String title, List<String> hulls, int max, float opad) {
		addBlueprintList(tooltip, title, hulls, max, opad, new BlueprintLister(){
			public boolean isKnown(String id) {
				if (Global.CODEX_TOOLTIP_MODE) return false;
				return Global.getSector().getPlayerFaction().knowsShip(id);
			}
			public String getNoun(int num) {
				if (num == 1) return "hull";
				return "hulls";
			}
			public String getName(String id) {
				ShipHullSpecAPI spec = Global.getSettings().getHullSpec(id);
				return spec.getNameWithDesignationWithDashClass();
			}
		});
	}
	
	protected void addWeaponList(TooltipMakerAPI tooltip, String title, List<String> weapons, int max, float opad) {
		addBlueprintList(tooltip, title, weapons, max, opad, new BlueprintLister(){
			public boolean isKnown(String id) {
				if (Global.CODEX_TOOLTIP_MODE) return false;
				return Global.getSector().getPlayerFaction().knowsWeapon(id);
			}
			public String getNoun(int num) {
				if (num == 1) return "weapon";
				return "weapons";
			}
			public String getName(String id) {
				WeaponSpecAPI spec = Global.getSettings().getWeaponSpec(id);
				return spec.getWeaponName();
			}
		});
	}
	
	protected void addFighterList(TooltipMakerAPI tooltip, String title, List<String> wings, int max, float opad) {
		addBlueprintList(tooltip, title, wings, max, opad, new BlueprintLister(){
			public boolean isKnown(String id) {
				if (Global.CODEX_TOOLTIP_MODE) return false;
				return Global.getSector().getPlayerFaction().knowsFighter(id);
			}
			public String getNoun(int num) {
				if (num == 1) return "fighter LPC";
				return "fighter LPCs";
			}
			public String getName(String id) {
				FighterWingSpecAPI spec = Global.getSettings().getFighterWingSpec(id);
				return spec.getWingName();
			}
		});
	}
	
	protected void addBlueprintList(TooltipMakerAPI tooltip, String title, List<String> ids, int max, float opad,
				BlueprintLister lister) {
		
		Color b = Misc.getButtonTextColor();
		Color g = Misc.getGrayColor();
		
		if (Global.CODEX_TOOLTIP_MODE) {
			tooltip.setParaSmallInsignia();
		}
		tooltip.addPara(title, opad);
		if (Global.CODEX_TOOLTIP_MODE) {
			tooltip.setParaFontDefault();
		}
		
		String tab = "        ";
		float small = 5f;
		float pad = small;
		
		int left = ids.size();
		
		List<String> copy = new ArrayList<String>();
		for (String id : ids) {
			if (!lister.isKnown(id)) copy.add(id);
		}
		for (String id : ids) {
			if (lister.isKnown(id)) copy.add(id);
		}
		
		ids = copy;
		for (String id : ids) {
			boolean known = lister.isKnown(id);
			
			if (known) {
				tooltip.addPara(tab + lister.getName(id) + " (known)", g, pad);
			} else {
				tooltip.addPara(tab + lister.getName(id), b, pad);
			}
			left--;
			pad = 3f;
			if (ids.size() - left >= max - 1) break;
		}
		if (ids.isEmpty()) {
			tooltip.addPara(tab + "None", pad);
		}
		if (left > 0) {
			String noun = lister.getNoun(left);
			tooltip.addPara(tab + "... and %s other " + noun + "", pad, Misc.getHighlightColor(), "" + left);
		}
	}
	public String getDesignType() {
		return spec.getManufacturer();
	}
	public SpecialItemSpecAPI getSpec() {
		return spec;
	}
	
//	@Override
//	public int getNumToRemove() {
//		return 1;
//	}
//	
//	@Override
//	public boolean shouldRemoveCalledAfterRightClickAction() {
//		return false;
//	}
//	@Override
//	public void setReadOnlyCargoForRightClickAction(CargoAPI cargo) {
//		// TODO Auto-generated method stub
//		
//	}
	
}







